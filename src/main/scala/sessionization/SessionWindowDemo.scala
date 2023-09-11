package sessionization

import com.ashu.sparknewfeatures.SparkFeatures.getSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, session_window, window}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SessionWindowDemo {
  def main(args: Array[String]): Unit = {
    val windowingData: Seq[Row] = List(
      Row("12", "2019-01-02 15:30:00"),
      Row("12",  "2019-01-02 15:30:30"),
      Row("12",  "2019-01-02 15:31:00"),
      Row("12",  "2019-01-02 15:31:50"),
      Row("12",  "2019-01-02 15:31:55"),
      Row("16",  "2019-01-02 15:33:00"),
      Row("16",  "2019-01-02 15:35:20"),
      Row("16",  "2019-01-02 15:37:00"),
      Row("20",  "2019-01-02 15:30:30"),
      Row("20",  "2019-01-02 15:31:00"),
      Row("20",  "2019-01-02 15:31:50"),
      Row("20",  "2019-01-02 15:31:55"),
      Row("20",  "2019-01-02 15:33:00"),
      Row("20",  "2019-01-02 15:35:20"),
      Row("20",  "2019-01-02 15:37:00"),
      Row("20",  "2019-01-02 15:40:00"),
      Row("20",  "2019-01-02 15:45:00"),
      Row("20",  "2019-01-02 15:46:00"),
      Row("20",  "2019-01-02 15:47:30"),
      Row("20",  "2019-01-02 15:48:00"),
      Row("20",  "2019-01-02 15:48:10"),
      Row("20",  "2019-01-02 15:48:20"),
      Row("20",  "2019-01-02 15:48:30"),
      Row("20",  "2019-01-02 15:50:00"),
      Row("20",  "2019-01-02 15:53:00"),
      Row("20",  "2019-01-02 15:54:30"),
      Row("20",  "2019-01-02 15:55:00"),
      Row("22",  "2019-01-02 15:50:30"),
      Row("22",  "2019-01-02 15:52:00"),
      Row("22",  "2019-01-02 15:50:30"),
      Row("22",  "2019-01-02 15:52:00"),
      Row("22",  "2019-01-02 15:50:30"),
      Row("22",  "2019-01-02 15:52:00"))


    val spark: SparkSession = getSparkSession
    val data: RDD[Row] = spark.sparkContext.parallelize(windowingData)
    val columns: StructType = StructType(List(StructField("eventId",StringType,nullable = true), StructField("timeReceived", StringType, nullable = true)))
    val windowing_df: DataFrame = spark.createDataFrame(data, columns)

    windowing_df.printSchema()
    windowing_df.show(50,truncate = false)

    //Tumbling window
    val tumblingWindows =
      windowing_df
        .withWatermark("timeReceived", "10 minutes")
        .groupBy(col("eventId"), window(col("timeReceived"), "10 minutes"))
        .count()

    println("Tumbling window of 10 minutes")
    tumblingWindows.show(50,truncate = false)

    //Sliding Window
    val slidingWindows =
      windowing_df
        .withWatermark("timeReceived", "10 minutes")
        .groupBy(col("eventId"), window(col("timeReceived"), "10 minutes", "5 minutes"))
        .count()

    println("Sliding window of 5 minutes")
    slidingWindows.show(50,truncate = false)

    //Session Window
    val sessionWindows =
      windowing_df
        //.withWatermark("timeReceived", "10 minutes")
        .groupBy(col("eventId"), session_window(col("timeReceived"), "5 minutes"))
        .count()

    println("Session window of 5 minutes gap duration")
    sessionWindows.show(50, truncate = false)


  }

}
