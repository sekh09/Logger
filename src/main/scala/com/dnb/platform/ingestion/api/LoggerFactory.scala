package com.dnb.platform.ingestion.api

import com.dnb.platform.ingestion.impl._
import com.dnb.platform.ingestion.exception.LoggerException
import com.dnb.platform.ingestion.constant.LoggerConstants._

object LoggerFactory {
  def apply(inputType: String) = inputType.toUpperCase match {
    case XML_FORMAT  => new XMLLogger()
    case JSON_FORMAT => new JSONLogger()
    case _           => throw new LoggerException("currently not supporting following format " + inputType)
  }
}