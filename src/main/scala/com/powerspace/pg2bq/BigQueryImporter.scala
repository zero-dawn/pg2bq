package com.powerspace.pg2bq

import com.powerspace.pg2bq.config.BigQueryConfiguration
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class BigQueryImporter(spark: SparkSession, tmpBucket: String, config: BigQueryConfiguration)
    extends LazyLogging
    with DataImporter {

  val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

  // ensure the dataset exists or create it
  getOrCreateDataset(config)

  override def createOrOverride(df: DataFrame, tableName: String): Unit = {
    saveIntoGcs(df, tableName)
    loadFromGcsToBq(tableName)
  }

  private def loadFromGcsToBq(tableName: String): Unit = {
    val configuration = LoadJobConfiguration
      .builder(TableId.of(config.dataset, tableName), s"gs://$tmpBucket/$tableName/*.avro")
      .setFormatOptions(FormatOptions.avro())
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .build()

    val job = bigquery.create(JobInfo.newBuilder(configuration).build())

    logger.info(s"Importing $tableName from bucket $tmpBucket to dataset ${config.dataset}...")
    job.waitFor()
    logger.info(s"$tableName import done!")
  }

  private def saveIntoGcs(df: DataFrame, tableName: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .save(s"gs://$tmpBucket/$tableName")
  }

  def getOrCreateDataset(config: BigQueryConfiguration): Dataset = {
    scala.Option(bigquery.getDataset(config.dataset)) match {
      case Some(ds) =>
        logger.info(s"Dataset ${config.dataset} already exist.")
        ds
      case None =>
        logger.info(s"Dataset ${config.dataset} does not exist, creating...")
        val infoBuilder = DatasetInfo.newBuilder(config.dataset)
        if (config.location.isDefined) {
          infoBuilder.setLocation(config.location.toString())
        }
        val ds = bigquery.create(infoBuilder.build())
        logger.info(s"Dataset ${config.dataset} created!")
        ds
    }
  }

}
