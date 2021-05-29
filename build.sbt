name := "BDATaxi"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.hadoop" % "hadoop-client" % "2.8.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.8",
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1",
  "io.spray" % "spray-json_2.11" % "1.3.3",
  "org.vegas-viz"%"vegas_2.11"%"0.3.11",
  "org.vegas-viz"%"vegas-spark_2.11"%"0.3.11")