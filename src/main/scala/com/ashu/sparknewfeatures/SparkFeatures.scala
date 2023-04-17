package com.ashu.sparknewfeatures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StringType, StructType}

object SparkFeatures{
  def getSparkSession: SparkSession = {
    SparkSession.builder.appName("Spark3.4FeaturesDemo").master("local[*]").getOrCreate()
  }

  /*
  Important Note: UPDATE, DELETE, MERGE may not work in your local IDE, but they are available on Databricks Runtime 13.0.
  SQL queries now support specifying default values for columns of tables in CSV, JSON, ORC, Parquet formats.
  This functionality works either at table creation time or afterwards. Subsequent INSERT, UPDATE, DELETE, and
  MERGE commands may thereafter refer to any column's default value using the explicit DEFAULT keyword. Or, if
  any INSERT assignment has an explicit list of fewer columns than the target table, corresponding column default
  values will be substituted for the remaining columns (or NULL if no default is specified).
   */
  def defaultValues(spark: SparkSession): Unit ={
    spark.sql("CREATE TABLE test_default_values (some_id INT, some_date DATE DEFAULT CURRENT_DATE()) USING PARQUET")
    spark.sql("INSERT INTO test_default_values VALUES (0, DEFAULT), (1, DEFAULT), (2, DATE'2020-12-31')")
    spark.sql("SELECT some_id, some_date FROM test_default_values").show()
  }

  /*
  Apache Spark 3.4 adds a new data type to represent timestamp values without a time zone. Until now, values expressed
  using Spark's existing TIMESTAMP data type as embedded in SQL queries or passed through JDBC were presumed to be in
  session local timezone and cast to UTC before being processed. While these semantics are desirable in several cases
  such as dealing with calendars, in many other cases users would rather express timestamp values independent of time
  zones, such as in log files. To this end, Spark now includes the new TIMESTAMP_NTZ data type.
   */
  def timeStampWithoutTimezone(spark: SparkSession): Unit ={
    spark.sql("CREATE TABLE test_timestamp (c1 TIMESTAMP_NTZ) USING PARQUET")
    spark.sql("INSERT INTO test_timestamp VALUES (TIMESTAMP_NTZ'2016-01-01 10:11:12.123456')")
    spark.sql("INSERT INTO test_timestamp VALUES (NULL)")
    spark.sql("SELECT c1 FROM test_timestamp").show(false)
  }

  /*
  In Apache Spark 3.4 it is now possible to use lateral column references in SQL SELECT lists to refer to previous items.
  This feature brings significant convenience when composing queries, often replacing the need to write complex sub-queries
  and common table expressions.
   */
  def columnAliasReference(spark: SparkSession): Unit ={
    spark.sql("CREATE TABLE test_column_alias_reference (salary INT, bonus INT, name STRING) USING PARQUET")
    spark.sql("INSERT INTO test_column_alias_reference VALUES (10000, 1000, 'ashutosh')")
    spark.sql("INSERT INTO test_column_alias_reference VALUES (20000, 500, 'kumar')")
    spark.sql("SELECT salary * 2 AS new_salary, new_salary + bonus FROM test_column_alias_reference WHERE name = 'ashutosh'").show(false)
  }

  /*
  Apache Spark 3.4 introduced a new API called Dataset.to(StructType) to convert the entire source dataframe to the specified
  schema. Its behavior is similar to table insertion where the input query is adjusted to match the table schema, but it's
  extended to work for inner fields as well. This includes:
  1. Reordering columns and inner fields to match the specified schema.
  2. Projecting away columns and inner fields not needed by the specified schema.
  3. Casting columns and inner fields to match the expected data types
   */
  def datasetToStructType(sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._

    val innerFields = new StructType().add("J", StringType).add("I", StringType)
    val schema = new StructType().add("struct", innerFields, nullable = false)
    val df = Seq("a" -> "b").toDF("i", "j").select(struct(col("i"), col("j")).as("struct")).to(schema)
    assert(df.schema == schema)
    val result = df.collect()
    println(result.mkString("Array(", ", ", ")"))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = getSparkSession
    defaultValues(spark)
    timeStampWithoutTimezone(spark)
    columnAliasReference(spark)
    datasetToStructType(spark)
  }
}