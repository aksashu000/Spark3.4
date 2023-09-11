package sessionization

import com.ashu.sparknewfeatures.SparkFeatures.getSparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, session_window, to_timestamp}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}

object StreamingSessionization {
  case class SessionData(sessionId: String, timestamp: String)


  def main(args: Array[String]): Unit = {

    val streamListener = new StreamingQueryListener() {

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        //println("Query made progress: " + queryProgress.progress)
        println("watermark " + queryProgress.progress.eventTime.get("watermark"))
      }

      // We don't want to do anything with start or termination, but we have to override them anyway
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = { }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = { }
    }

    val spark: SparkSession = getSparkSession

    spark.streams.addListener(streamListener)

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._

    val windowing_df = lines.map(x => SessionData(x.getString(0).split("  ")(0), x.getString(0).split("  ")(1)))

    val sessionWindows = windowing_df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withWatermark("timestamp", "20 minutes")
        .groupBy(col("sessionId"), session_window(col("timestamp"), "60 seconds"))
        .count()

    sessionWindows.printSchema()

    sessionWindows
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Append())
      .foreachBatch((df: DataFrame, batchId: Long) => {
        df.show(500, false)
      })
      .start()
      .awaitTermination()

  }
}
