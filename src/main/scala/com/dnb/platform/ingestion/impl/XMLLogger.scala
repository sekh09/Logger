package com.dnb.platform.ingestion.impl

import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import com.dnb.platform.ingestion.api.Logger
import com.dnb.platform.ingestion.constant.LoggerConstants._
import org.apache.spark.sql.functions.{ col, lit }
import com.dnb.platform.ingestion.util.LoggerUtility
import org.apache.spark.sql.functions.broadcast
import com.dnb.platform.ingestion.util.LoggingHolder

class XMLLogger extends Logger {

  override def readInputFiles(spark: SparkSession, path: String): Try[DataFrame] = {
    Try {
      LoggingHolder.LOG.info("XML path :" + path)
      val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "activity").xml(path)
      df
    }
  }

  /**
   * Joined with broadcasted dataframe of activity.csv
   * filtering out required columns only
   * conversion of date format to timestamp
   */

  override def getOutputSchemaCompatibleDataframe(spark: SparkSession, inputDF: DataFrame): DataFrame = {
    var updatedDF = inputDF.select(RENAME_XML_COLS.map(x => col(x._1).alias(x._2)).toList: _*)
    val lookupDF = loadActivityLookUpFile(spark, System.getProperty("activity.lookup.file.path")).get
    updatedDF = updatedDF.withColumn("signedInTime", LoggerUtility.convertUDF(updatedDF("signedInTime"), lit(XML_DATE_FORMAT)))
    updatedDF = updatedDF.join(broadcast(lookupDF), Seq(ACTIVITY_CODE), "left_outer") // join with broadcasted DF
    updatedDF = updatedDF.withColumnRenamed("activity_description", "activityTypeDescription")
    updatedDF = updatedDF.select(OUTPUT_COLS.head, OUTPUT_COLS.tail: _*)
    updatedDF
  }

}