package com.dnb.platform.ingestion.impl

import com.dnb.platform.ingestion.constant.LoggerConstants.JSON_DATE_FORMAT
import com.dnb.platform.ingestion.constant.LoggerConstants.JSON_FORMAT
import com.dnb.platform.ingestion.constant.LoggerConstants.OUTPUT_COLS
import com.dnb.platform.ingestion.constant.LoggerConstants.RENAME_JSON_COLS
import com.dnb.platform.ingestion.api.CommonFlatSpec
import com.dnb.platform.ingestion.api.LoggerFactory

class JSONLoggerSpec extends CommonFlatSpec {

  behavior of "JSONLogger"

  val logger = LoggerFactory(JSON_FORMAT)

  val inputPath = "src/test/resources/input.json"

  it should "get column names as expected" in {
    val df = logger.readInputFiles(spark, inputPath).get
    df.show
    val columns = df.columns
    val expectedColNames = RENAME_JSON_COLS.keys.toArray
    val cols = columns.diff(expectedColNames)
    assert(cols.length === 0) //remaining column name after all column names matched as not considering for output schema
  }

  it should "updated column names as output schema" in {
    val df = logger.readInputFiles(spark, inputPath).get
    val joinedDF = logger.getOutputSchemaCompatibleDataframe(spark, df) //updating schema to sync with expected output
    joinedDF.show
    val res = joinedDF.columns.diff(OUTPUT_COLS)
    assert(res.length === 0) 
  }

}