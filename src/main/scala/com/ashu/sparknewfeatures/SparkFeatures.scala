package com.ashu.sparknewfeatures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkFeatures{
  def getSparkSession: SparkSession = {
    SparkSession.builder.appName("Spark3.4FeaturesDemo").master("local[*]").getOrCreate()
  }

  def defaultValuesDemo(spark: SparkSession): Unit ={
    spark.sql("CREATE TABLE test (some_id INT, some_date DATE DEFAULT CURRENT_DATE()) USING PARQUET")
    spark.sql("INSERT INTO test VALUES (0, DEFAULT), (1, DEFAULT), (2, DATE'2020-12-31')")
    spark.sql("SELECT some_id, some_date FROM test").show()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = getSparkSession
    defaultValuesDemo(spark)
  }
}