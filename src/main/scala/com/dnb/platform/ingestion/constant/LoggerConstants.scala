package com.dnb.platform.ingestion.constant

/**
 * Generic Constants
 */
object LoggerConstants {
  val COMMA_DELIMITER = ","
  val NEW_LINE_SEPARATOR = "\n"
  val EMPTY_SPACE = ""
  val SINGLE_SPACE = " "
  val PIPE = "|"
  val ACTIVITY_CODE="activity_code"
  val RENAME_XML_COLS = Map("userName" -> "user", "websiteName" -> "website", "activityTypeCode" -> ACTIVITY_CODE,"loggedInTime" -> "signedInTime")
  val RENAME_JSON_COLS = Map("userName" -> "user", "websiteName" -> "website", "activityTypeDescription" -> "activityTypeDescription","signedInTime" -> "signedInTime")
  val OUTPUT_COLS = Array("user","website","activityTypeDescription","signedInTime")
  val JSON_DATE_FORMAT="MM/dd/yyyy"
  val XML_DATE_FORMAT="yyyy-MM-dd"
  val XML_FORMAT="XML"
  val JSON_FORMAT="JSON"

}