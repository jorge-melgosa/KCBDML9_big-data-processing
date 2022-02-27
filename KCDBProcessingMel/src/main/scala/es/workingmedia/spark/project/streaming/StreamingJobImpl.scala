package es.workingmedia.spark.project.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {

    val struct = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")

  }

  override def computeBytesReceivedPerAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "90 seconds"))
      .agg(
        sum("bytes").as("sum_bytes_antenna")
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id") , $"sum_bytes_antenna".as("value"))
        .withColumn("type", lit("antenna_bytes_total"))
  }

  override def computeBytesTransmittedPerUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "90 seconds"))
      .agg(
        sum("bytes").as("sum_bytes_user")
      )
      .select($"window.start".as("timestamp"), $"id", $"sum_bytes_user".as("value"))
        .withColumn("type", lit("user_bytes_total"))
  }

  override def computeBytesTransmittedPerApplication(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "90 seconds"))
      .agg(
        sum("bytes").as("sum_bytes_app")
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"sum_bytes_app".as("value"))
        .withColumn("type", lit("app_bytes_total"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/project/spark-checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val kafkaServer = "35.226.98.13:9092"
    val topic = "devices"
    val jdbcUri = s"jdbc:postgresql://34.88.135.235:5432/postgres"
    val aggJdbcTable = "bytes"
    val jdbcUser = "postgres"
    val jdbcPassword = "demo"
    val storagePath = "/tmp/project/data-spark"

    run(
      Array(
        kafkaServer,
        topic,
        jdbcUri,
        aggJdbcTable,
        jdbcUser,
        jdbcPassword,
        storagePath
      )
    )

  }

}
