package com.dnb.platform.ingestion.api

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

/*
 * Base test class for all test cases
 */

abstract class CommonFlatSpec extends AnyFlatSpec with BeforeAndAfterAll{

   var spark:SparkSession = null
   
   override def beforeAll() {
      spark= SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate() // An existing SparkContext.
  }
}
