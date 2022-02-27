package es.workingmedia.spark.project.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class MobileDevicesMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, antenna_id: String, bytes: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame
  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame
  def enrichDeviceWithUserMetadata(deviceDF: DataFrame, userMetadataDF: DataFrame): DataFrame

  def computeBytesReceivedPerAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedPerUserMail(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedPerApplication(dataFrame: DataFrame): DataFrame
  def computeUserWhoHaveExceededTheHourlyQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTableHour, aggJdbcTableLimit, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val deviceDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val deviceMetadataDF = enrichDeviceWithUserMetadata(deviceDF, metadataDF).cache()

    val aggReceivedPerAntennaDF = computeBytesReceivedPerAntenna(deviceMetadataDF)
    val aggTransmittedPerUserDF = computeBytesTransmittedPerUserMail(deviceMetadataDF)
    val aggTransmittedPerApplicationDF = computeBytesTransmittedPerApplication(deviceMetadataDF)
    val aggUserExceededTheHourlyQuotaDF = computeUserWhoHaveExceededTheHourlyQuota(deviceMetadataDF)

    writeToJdbc(aggReceivedPerAntennaDF, jdbcUri, aggJdbcTableHour, jdbcUser, jdbcPassword)
    writeToJdbc(aggTransmittedPerUserDF, jdbcUri, aggJdbcTableHour, jdbcUser, jdbcPassword)
    writeToJdbc(aggTransmittedPerApplicationDF, jdbcUri, aggJdbcTableHour, jdbcUser, jdbcPassword)
    writeToJdbc(aggUserExceededTheHourlyQuotaDF, jdbcUri, aggJdbcTableLimit, jdbcUser, jdbcPassword)

    spark.close()
  }
}
