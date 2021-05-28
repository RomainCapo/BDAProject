/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */


import GeoJsonProtocol.{FeatureCollectionJsonFormat, IntJsonFormat, StringJsonFormat}

import scala.math.max
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.esri.core.geometry.Point
import org.apache.spark.SparkContext
import spray.json._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit

class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
}

case class Trip(
                 license: String,
                 pickupTime: Long,
                 dropoffTime: Long,
                 pickupX: Double,
                 pickupY: Double,
                 dropoffX: Double,
                 dropoffY: Double,
                 vendorId: String,
                 tripTimeSecs: Int,
                 tripDistance: Double,
                 passengerCount: Int,
                 paymentType:String,
                 tipAmount:Double,
                 totalAmount:Double)

object RunGeoTime extends Serializable {

  def main(args: Array[String]): Unit = {
    // Spark initialisation
    val spark: SparkSession = SparkSession.builder
      .appName("BDA")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._

    // Default provided code
    val taxiRaw = spark.read.option("header", "true").csv("all_data.csv")
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.map(_.left.get).toDS
    taxiGood.cache()

    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)

    //taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

    // register the UDF, use it in a where clause
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

    val geojson = scala.io.Source.
      fromFile("nyc-boroughs.geojson").
      mkString

    val features = geojson.parseJson.convertTo[FeatureCollection]
    val areaSortedFeatures = features.sortBy { f =>
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    }

    val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("borough").convertTo[String]
      }).getOrElse("NA")
    }
    val boroughUDF = udf(bLookup)

    taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
    val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")
    //taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

    taxiGood.unpersist()

    val sessions = taxiDone.
      repartition($"license").
      sortWithinPartitions($"license", $"pickupTime").
      cache()
    def boroughDuration(t1: Trip, t2: Trip): (String, Long, String) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d, t1.license )
    }

    val boroughDurations: DataFrame =
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds", "license")
    /*boroughDurations.
      where("seconds > 0").
      groupBy("borough").
      agg(avg("seconds"), stddev("seconds")).
      show()*/

    boroughDurations.unpersist()

    // Question 1 - Potential conflict

    /*val quantileOnePct = boroughDurations.
      selectExpr("seconds").
      where("seconds > 0  AND seconds < 60*60*4").
      stat.approxQuantile("seconds", Array(0.05), 0.0)(0)

    println($"")



    val potentialConflictsByLicense = boroughDurations.
      selectExpr("seconds", "license").
      where(s"seconds > 0 AND seconds < $quantileOnePct").
      groupBy("license").
      count().
      sort($"count".desc)

    potentialConflictsByLicense.show()*/

    // Question 2 - MLlib
    val preproccedData = sessions.
      withColumn("hour",date_format($"pickupTime".cast("timestamp"), "HH")).
      withColumn("weekday",date_format($"pickupTime".cast("timestamp"), "E")).
      withColumn("dropoffBorough", boroughUDF($"dropoffX", $"dropoffY")).
      withColumn("pickupBorough", boroughUDF($"pickupX", $"pickupY")).
      withColumn("fareAmount", col("totalAmount") - col("tipAmount"))

    val rForm = new RFormula().setFormula("tipAmount ~ vendorId + tripTimeSecs + tripDistance + passengerCount + paymentType + hour + weekday + dropoffBorough + pickupBorough + fareAmount")

    val Array(train, test) = preproccedData.randomSplit(Array(0.7, 0.3))

    train.show()
    val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features") //Step 2

    val stages = Array(rForm, lr)
    val pipeline = new Pipeline().setStages(stages)

    val params = new ParamGridBuilder()
      //.addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.regParam, Array(0.1))
      .build()

    val evaluatorRMSE = new RegressionEvaluator().
      setMetricName("rmse").
      setLabelCol("label").
      setPredictionCol("prediction")

    val evaluatorMAE = new RegressionEvaluator().
      setMetricName("mae").
      setLabelCol("label").
      setPredictionCol("prediction")

    val tvs = new TrainValidationSplit()
      .setTrainRatio(0.75)
      .setEstimatorParamMaps(params)
      .setEstimator(pipeline)
      .setEvaluator(evaluatorRMSE)
      .setEvaluator(evaluatorMAE)

    val tvsFitted = tvs.fit(train)
    val preds = tvsFitted.transform(test)


    val predsPos = preds.withColumn("prediction", when(col("prediction") > 0, col("prediction")).otherwise(0))
      predsPos.show()

    println("RMSE : " + evaluatorRMSE.evaluate(preds))
    println("MAE : " + evaluatorMAE.evaluate(preds))

  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  def parseDoubleField(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  def parseIntField(rr: RichRow, locField: String, defaultVal:Int): Int = {
    rr.getAs[String](locField).map(_.toInt).getOrElse(defaultVal)
  }

  def parse(line: Row): Trip = {
    val rr = new RichRow(line)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseDoubleField(rr, "pickup_longitude"),
      pickupY = parseDoubleField(rr, "pickup_latitude"),
      dropoffX = parseDoubleField(rr, "dropoff_longitude"),
      dropoffY = parseDoubleField(rr, "dropoff_latitude") ,
      vendorId = rr.getAs[String]("vendor_id").orNull,
      tripTimeSecs = parseIntField(rr, "trip_time_in_secs", 0),
      tripDistance = parseDoubleField(rr, "trip_distance"),
      passengerCount= parseIntField(rr, "passenger_count", 1),
      paymentType = rr.getAs[String]("payment_type").orNull,
      tipAmount = parseDoubleField(rr, "tip_amount"),
      totalAmount = parseDoubleField(rr, "total_amount")
    )
  }
}

/*case class Trip(
                 license: String,
                 pickupTime: Long,
                 dropoffTime: Long,
                 pickupX: Double,
                 pickupY: Double,
                 dropoffX: Double,
                 dropoffY: Double,
                 vendorId: String,
                 tripTimeSecs: Int,
                 tripDistance: Int,
                 passengerCount: Int,
                 paymentType:String,
                 tipAmount:Double,
                 totalAmount:Double)*/