package com.dnb.platform.ingestion.api

import org.apache.spark.sql.SparkSession
import com.dnb.platform.ingestion.constant.LoggerConstants._
import com.dnb.platform.ingestion.exception.LoggerException
import com.dnb.platform.ingestion.util.LoggingHolder
import com.dnb.platform.ingestion.util.PropertyFileLoader

object Driver {
  case class LoggerConfig(
    inputPath:   String = "",
    outputPath:  String = "",
    format:      String = "",
    repartition: Int    = 0) // repartition count can be decided on input file records/size

  def main(args: Array[String]): Unit = {
    parseConfig(args) match {
      case Some(config) =>
        //Set the configuration
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        spark.sparkContext.getConf.setAppName("Logger_Data_Ingestion")
        LoggingHolder.LOG.info("Logger Data Ingestion has been started")
        processIngestion(config, spark)

    }
  }
  def parseConfig(args: Array[String]) = {
    val parser = new scopt.OptionParser[LoggerConfig]("Logger_Data_Ingestion") {
      opt[String]('i', "inputPath").required().valueName("").action((x, c) => c.copy(inputPath = x)).text("Input  File Path is a required Field")
      opt[String]('o', "outputPath").required().valueName("").action((x, c) => c.copy(outputPath = x)).text("Output File Path is a required field")
      opt[String]('f', "format").required().valueName("").action((x, c) => c.copy(format = x)).text("format is required field, currently supports XML/JSON")
      opt[String]('r', "repartition").required().valueName("").action((x, c) => c.copy(repartition = x.toInt)).text("number of Partitions is a required field")
    }
    parser.parse(args, LoggerConfig())
  }

  def processIngestion(config: LoggerConfig, spark: SparkSession) = {
    try {
      PropertyFileLoader.loadPropertiesFile() //in case any environment specific variables
      val logger = LoggerFactory(config.format.trim) //factory design pattern
      val inputDF = logger.readInputFiles(spark, config.inputPath.trim) //reading of input file (xml/json)
      inputDF.get.show()
      if (inputDF.isSuccess && !inputDF.get.isEmpty) {
        val joinDF = logger.getOutputSchemaCompatibleDataframe(spark, inputDF.get) //updating schema to sync with expected output
        logger.writeOutPutFile(spark, config.outputPath.trim, joinDF, config.repartition)
        //can extend the functionality to generate reports and statistics
      } else {
        throw new LoggerException("Failed to read input file");
      }
    } catch {
      case e: Exception =>
        LoggingHolder.LOG.error("Error while running Logger App : " + e.getMessage);
        throw new LoggerException("Error while running Logger App : " + e.getMessage);

    }
  }
}