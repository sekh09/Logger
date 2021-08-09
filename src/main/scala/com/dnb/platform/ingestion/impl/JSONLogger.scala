package com.dnb.platform.ingestion.impl

import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import com.dnb.platform.ingestion.constant.LoggerConstants._
import com.dnb.platform.ingestion.api.Logger
import com.dnb.platform.ingestion.util.LoggerUtility
import com.dnb.platform.ingestion.util.LoggingHolder

class JSONLogger extends Logger {

  override def readInputFiles(spark: SparkSession, path: String): Try[DataFrame] = {
    Try {
      LoggingHolder.LOG.info("JSON path :"+path)
      val df = spark.read.option("multiline", "true").json(path)
      df
    }
  }

  override def getOutputSchemaCompatibleDataframe(spark: SparkSession, inputDF: DataFrame): DataFrame = {
    var updatedDF = inputDF.select(RENAME_JSON_COLS.map(x => col(x._1).alias(x._2)).toList: _*)
    updatedDF = updatedDF.withColumn("signedInTime", LoggerUtility.convertUDF(updatedDF("signedInTime"), lit(JSON_DATE_FORMAT)))
    updatedDF
  }

}
