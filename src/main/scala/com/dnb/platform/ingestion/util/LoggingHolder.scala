package com.dnb.platform.ingestion.util

import org.apache.log4j.Logger

object LoggingHolder extends Serializable {
  
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

}
