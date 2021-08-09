package com.dnb.platform.ingestion.api

import org.apache.spark.sql.{ SaveMode, DataFrame }
import org.apache.spark.sql.SparkSession
import com.dnb.platform.ingestion.constant.LoggerConstants._
import scala.util.Try

trait Logger {

  def readInputFiles(spark: SparkSession, path: String): Try[DataFrame]

  def loadActivityLookUpFile(spark: SparkSession, path: String): Try[DataFrame] = {
    Try {
      val lookupDataset = spark.read.option("header", "true")
        .option("delimiter", PIPE).option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true).csv(path);
      lookupDataset
    }
  }

  def writeOutPutFile(spark: SparkSession, outputPath: String, df: DataFrame, repartCount: Int) = {
    df.repartition(repartCount).write.mode(SaveMode.Overwrite).json(outputPath);
  }
  
  def getOutputSchemaCompatibleDataframe(spark: SparkSession,inputDf:DataFrame):DataFrame

}