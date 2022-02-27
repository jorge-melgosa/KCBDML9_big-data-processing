package es.workingmedia.spark.project.batch
import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where($"year" === lit(filterDate.getYear) &&
        $"month" === lit(filterDate.getMonthValue) &&
        $"day" === lit(filterDate.getDayOfMonth) &&
        $"hour" === lit(filterDate.getHour)
      )
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDeviceWithUserMetadata(deviceDF: DataFrame, userMetadataDF: DataFrame): DataFrame = {
    deviceDF.as("a")
      .join(userMetadataDF.as("b"), $"a.id" === $"b.id")
      .drop($"b.id")
  }

  override def computeBytesReceivedPerAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .groupBy($"antenna_id", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("sum_bytes_antenna")
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"sum_bytes_antenna".as("value"))
      .withColumn("type", lit("antenna_bytes_total"))
  }

  override def computeBytesTransmittedPerUserMail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("sum_bytes_email")
      )
      .select($"window.start".as("timestamp"), $"email".as("id"), $"sum_bytes_email".as("value"))
      .withColumn("type", lit("email_bytes_total"))
  }

  override def computeBytesTransmittedPerApplication(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("sum_bytes_app")
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"sum_bytes_app".as("value"))
      .withColumn("type", lit("app_bytes_total"))
  }

  override def computeUserWhoHaveExceededTheHourlyQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes", $"quota")
      .groupBy($"email", $"quota", window($"timestamp", "1 hour"))
      .agg(
        sum("bytes").as("sum_bytes_email")
      )
      .select($"email", $"sum_bytes_email".as("usage"), $"quota", $"window.start".as("timestamp"))
      .where($"sum_bytes_email" > $"quota")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  def main(args: Array[String]): Unit = {

    val filterDate = "2022-02-26T00:00:01Z"
    val storagePath = "/tmp/project/data-spark"
    val jdbcUri = s"jdbc:postgresql://34.88.135.235:5432/postgres"
    val jdbcMetadataTable = "user_metadata"
    val aggJdbcTableHour = "bytes_hourly"
    val aggJdbcTableLimit = "user_quota_limit"
    val jdbcUser = "postgres"
    val jdbcPassword = "demo"


    run(
      Array(
        filterDate,
        storagePath,
        jdbcUri,
        jdbcMetadataTable,
        aggJdbcTableHour,
        aggJdbcTableLimit,
        jdbcUser,
        jdbcPassword
      )
    )

  }
}
