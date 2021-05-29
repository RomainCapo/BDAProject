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
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, RegressionModel}
import vegas.{AggOps, Bar, Bin, Nom, Quant, Quantitative}
import vegas.DSL.Vegas
import vegas.data.External.{Cars, Movies}
import vegas.sparkExt.VegasSpark
import vegas.spec.Spec.Scale
import vegas.spec.Spec.TypeEnums.Nominal

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

    //taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
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
    // Question 1.1
    val quantileOnePct = boroughDurations.
      selectExpr("seconds").
      where("seconds > 0  AND seconds < 60*60*4").
      stat.approxQuantile("seconds", Array(0.05), 0.0)(0)


    val potentialConflictsByLicense = boroughDurations.
      selectExpr("seconds", "license", "borough").
      where(s"seconds > 0 AND seconds < $quantileOnePct").
      groupBy("license").
      count().
      sort($"count".desc)

    /*potentialConflictsByLicense.show()
    potentialConflictsByLicense.createOrReplaceTempView("counts")
    val countSum = potentialConflictsByLicense.sqlContext.sql("select sum(count) as countSum from counts").map(row => row.getAs("countSum").asInstanceOf[Long]).collect()(0)
    println("All potential conflict: " + countSum)*/

    /*boroughDurations.
      selectExpr("seconds", "license", "borough").
      where(s"seconds > 0 AND seconds < $quantileOnePct")
      .groupBy("borough").count().show()*/

    // Question 1.2
    val (startValues,counts) = sessions.select("tipAmount").where(s"tipAmount <= 20 ").map(value => value.getDouble(0)).rdd.histogram(15)

    val avg = sessions.select(mean("tipAmount")).first().get(0)

    println("Mean of tip amount : " + avg)

    /*val evaluatorRMSE = new RegressionEvaluator().
      setMetricName("rmse").
      setLabelCol("label").
      setPredictionCol("prediction")

    val evaluatorMAE = new RegressionEvaluator().
      setMetricName("mae").
      setLabelCol("label").
      setPredictionCol("prediction")*/

    /*val dfTips = sessions.select("tipAmount").withColumn("prediction", lit(avg)).withColumnRenamed("tipAmount", "label")

    println("Baseline RMSE : " + evaluatorRMSE.evaluate(dfTips))
    println("Baseline MAE : " + evaluatorMAE.evaluate(dfTips))*/

    // Question 2 - MLlib
    // Question 2.1
    /*val preproccedData = sessions.
      withColumn("hour",date_format($"pickupTime".cast("timestamp"), "HH")).
      withColumn("weekday",date_format($"pickupTime".cast("timestamp"), "E")).
      withColumn("dropoffBorough", boroughUDF($"dropoffX", $"dropoffY")).
      withColumn("pickupBorough", boroughUDF($"pickupX", $"pickupY")).
      withColumn("fareAmount", col("totalAmount") - col("tipAmount"))

    val rForm = new RFormula().setFormula("tipAmount ~ vendorId + tripTimeSecs + tripDistance + passengerCount + paymentType + hour + weekday + dropoffBorough + pickupBorough + fareAmount")

    val Array(train, test) = preproccedData.randomSplit(Array(0.7, 0.3))

    train.show()
    val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features") //Step 2


    val LrStages = Array(rForm, lr)
    val lrPipeline = new Pipeline().setStages(LrStages)

    val lrParams = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.regParam, Array(0.1))
      .build()


    val lrTvs = new TrainValidationSplit()
      .setTrainRatio(0.75)
      .setEstimatorParamMaps(lrParams)
      .setEstimator(lrPipeline)
      .setEvaluator(evaluatorRMSE)
      .setEvaluator(evaluatorMAE)

    val lrTvsFitted = lrTvs.fit(train)
    val lrPreds = lrTvsFitted.transform(test)


    val lrPredsPos = lrPreds.withColumn("prediction", when(col("prediction") > 0, col("prediction")).otherwise(0))
    lrPredsPos.show()

    println("RMSE : " + evaluatorRMSE.evaluate(lrPredsPos))
    println("MAE : " + evaluatorMAE.evaluate(lrPredsPos))
    // Question 2.2

    val rfr = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features") //Step 2


    val rfrStages = Array(rForm, rfr)
    val rfrPipeline = new Pipeline().setStages(rfrStages)

    val rfrParams = new ParamGridBuilder()
      .addGrid(rfr.maxDepth, Array(1,2,5))
      .addGrid(rfr.maxBins, Array(16,32,64))
      .build()


    val rfrTvs = new TrainValidationSplit()
      .setTrainRatio(0.75)
      .setEstimatorParamMaps(lrParams)
      .setEstimator(rfrPipeline)
      .setEvaluator(evaluatorRMSE)
      .setEvaluator(evaluatorMAE)

    val rfrTvsFitted = rfrTvs.fit(train)
    val rfrPreds = rfrTvsFitted.transform(test)


    val rfrPredsPos = rfrPreds.withColumn("prediction", when(col("prediction") > 0, col("prediction")).otherwise(0))
    rfrPredsPos.show()

    println("RMSE : " + evaluatorRMSE.evaluate(rfrPredsPos))
    println("MAE : " + evaluatorMAE.evaluate(rfrPredsPos))*/

    //Question 3
    /*val ScatterColorPlot =
      Vegas().
        withURL(Cars).
        mark(vegas.Point).
        encodeX("Horsepower", Quantitative).
        encodeY("Miles_per_Gallon", Quantitative).
        encodeColor(field="Origin", dataType=Nominal)
    ScatterColorPlot.show*/
    //sessions.select(col("pickupX"), col("pickupY")).show()

    val dfScatter = sessions.select(col("dropoffX"), col("dropoffY"), col("tipAmount")).
      where("dropoffX > -74.2 and dropoffX < -73.6 and dropoffY > 40.5 and dropoffY < 41")

    val maxVal = 1000.0
    val bucketizer = new Bucketizer()
      .setInputCol("tipAmount")
      .setOutputCol("tipBucket")
      .setSplits(startValues.toArray :+ maxVal)

    val dataBucketized = bucketizer.transform(dfScatter)


    val a = Vegas(width = 1200.0, height = 1200.0).
      withDataFrame(dataBucketized).
      mark(vegas.Point).
      encodeX("dropoffX", Quantitative, scale = vegas.Scale(domainValues = List( -74.3,-73.6))).
      encodeY("dropoffY", Quantitative, scale = vegas.Scale(domainValues = List(40.5, 41.0))).
      encodeColor(field="tipBucket", dataType=Nominal)
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