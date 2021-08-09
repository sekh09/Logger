package com.dnb.platform.ingestion.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf

import com.dnb.platform.ingestion.constant.LoggerConstants._

object LoggerUtility {

  def getTimestamp(date: String, inputFormat: String): Timestamp = {
    var res = date match {
      case "" => None
      case _ => {
        val format = new SimpleDateFormat(inputFormat)
        Try(new Timestamp(format.parse(date).getTime)) match {
          case Success(t) => Some(t)
          case Failure(_) => None
        }
      }
    }
    res.getOrElse(null)
  }

  val convertUDF = udf(getTimestamp _)
}