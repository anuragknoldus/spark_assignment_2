name := "gargantua-spark-assignment2"

version := "1.0"

val spark = "org.apache.spark" % "spark-core_2.11" % "2.0.0"
val spark_sql = "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
val spark_hive = "org.apache.spark" % "spark-hive_2.11" % "2.0.0"
val configuration = ""

lazy val commonSettings = Seq(
  organization := "com.knoldus",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "gargantua-spark-assignment2",
    libraryDependencies ++= Seq(spark,spark_sql,spark_hive)
  )
