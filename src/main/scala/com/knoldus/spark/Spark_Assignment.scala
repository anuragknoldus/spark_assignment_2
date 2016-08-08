package com.knoldus.spark

import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, Row, DataFrame, SparkSession}

object Spark_Assignment extends App {


  val spark = SparkSession.builder().appName("Spark_Assignment").master("local")
  .enableHiveSupport().getOrCreate()


  val data = spark.read.format("com.databricks.spark.csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("/home/anurag/Desktop/Fire_Department_Calls_for_Service.csv")

  val sqlContext = spark.sqlContext

  val countOfDifferentNumbersCall = data.groupBy("Call Type").count.count()

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 1. Different types of calls were made to the Fire Department " +
  "!!!!!!!!!!!!!!!!!!!!!!!!!!! : " + countOfDifferentNumbersCall + "\n")

  val callTypeWithCount = data.select("Incident Number", "Call Type").groupBy("Call Type").count

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 2. Count of Incidents of each call type were there " +
  "!!!!!!!!!!!!!!!!!!!!!!!!!!! : " + callTypeWithCount.show + "\n")

  val oldestYear = data.select("Call Date").reduce {
    (a, b) =>
      if (year(a.getString(0)) < year(b.getString(0))) a else b
  }
  val latestYear = data.select("Call Date").reduce {
    (a, b) =>
      if (year(a.getString(0)) > year(b.getString(0))) a else b
  }

  def year(date: String): Int = {
    val year = date.substring(date.length - 4, date.length).toInt
    year
  }

  import spark.implicits._

  val totalNoOfYear = year(latestYear.getString(0)) - year(oldestYear.getString(0))
  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 3. Years of Fire Service Calls in the data file : " +
    "!!!!!!!!!!!!!!!!!!!!!!!!!!! : " + totalNoOfYear + "\n")

  implicit val dateEncoders = Encoders.DATE

  val udfForDate: UserDefinedFunction = udf((date: String) => {
    val formatter = new SimpleDateFormat("MM/dd/yyyy")
    new java.sql.Date(formatter.parse(date).getTime)
  })

  val dataSet = data.select("Call Date")
  val dateDataSet = dataSet.withColumn("Call Date", udfForDate(dataSet("Call Date"))).as[Date].orderBy($"Call Date".desc)
  val latestDate = dateDataSet.first()
  val sevenDaysBefore = (7 * 24 * 3600 * 1000).toLong
  val previousDate = new Date(latestDate.getTime - sevenDaysBefore)
  val totalCallInSevenDays = dateDataSet.filter(date => date.getTime > previousDate.getTime).count()
  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 4. Total call in seven days : " +
    "!!!!!!!!!!!!!!!!!!!!!!!!!!! : " + totalCallInSevenDays + "\n")

  data.select("city", "Neighborhood  District", "Call Date").createOrReplaceTempView("callerRecord")
  val mostFrequentlyCaller = sqlContext.sql("select * from callerRecord where city='San Francisco'")
    .filter(row => row.getString(2).contains("2015"))
    .groupBy("Neighborhood  District")
    .count()
    .reduce((a, b) => if (a.getLong(1) > b.getLong(1)) a else b)
  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 5. Most frequently caller from the San Francisco  : " +
    "!!!!!!!!!!!!!!!!!!!!!!!!!!! : " + mostFrequentlyCaller.getString(0) + "\n")

  spark.stop()

}
