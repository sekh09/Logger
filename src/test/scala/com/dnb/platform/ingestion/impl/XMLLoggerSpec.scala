package com.dnb.platform.ingestion.impl

import java.util.{ List, LinkedHashMap }
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ lit, col }
import com.dnb.platform.ingestion.constant.LoggerConstants._
import com.dnb.platform.ingestion.util.LoggingHolder
import com.dnb.platform.ingestion.util.LoggerUtility
import com.dnb.platform.ingestion.util.PropertyFileLoader
import com.dnb.platform.ingestion.api.CommonFlatSpec
import com.dnb.platform.ingestion.api.LoggerFactory


class XMLLoggerSpec extends CommonFlatSpec {

  behavior of "XMLLogger"

  val logger = LoggerFactory(XML_FORMAT)

  val inputPath = "src/test/resources/input.xml"

  it should "get column names as expected" in {
    val df = logger.readInputFiles(spark, inputPath).get
    df.show
    val columns = df.columns
    val expectedColNames = RENAME_XML_COLS.keys.toArray
    val cols = columns.diff(expectedColNames)
    assert(cols(0).equalsIgnoreCase("number_of_views")) //remaining column name after all column names matched as not considering for output schema
  }

  it should "updated column names as output schema" in {
    val df = logger.readInputFiles(spark, inputPath).get
    System.setProperty("activity.lookup.file.path", "src/test/resources/activity.csv")
    val joinedDF = logger.getOutputSchemaCompatibleDataframe(spark, df) //updating schema to sync with expected output
    joinedDF.show
    val res = joinedDF.columns.diff(OUTPUT_COLS)
    assert(res.length === 0) 
  }

}