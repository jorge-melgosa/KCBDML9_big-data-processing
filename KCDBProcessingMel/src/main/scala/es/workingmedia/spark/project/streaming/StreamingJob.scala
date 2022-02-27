package es.workingmedia.spark.project.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

//case class MobileDevicesMessage(timestamp: Timestamp, id: String, antenna_id: String, bytes: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame
  def parserJsonData(dataFrame: DataFrame): DataFrame

  def computeBytesReceivedPerAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedPerUser(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedPerApplication(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val deviceDF = parserJsonData(kafkaDF)

    val aggReceivedPerAntennaDF = computeBytesReceivedPerAntenna(deviceDF)
    val aggTransmittedPerUserDF = computeBytesTransmittedPerUser(deviceDF)
    val aggTransmittedPerApplicationDF = computeBytesTransmittedPerApplication(deviceDF)

    val storageFuture = writeToStorage(deviceDF, storagePath)

    val aggFuturePerAntenna = writeToJdbc(aggReceivedPerAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFuturePerUser = writeToJdbc(aggTransmittedPerUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFuturePerApplication = writeToJdbc(aggTransmittedPerApplicationDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(storageFuture, aggFuturePerAntenna, aggFuturePerUser, aggFuturePerApplication)), Duration.Inf)

    spark.close()
  }
}