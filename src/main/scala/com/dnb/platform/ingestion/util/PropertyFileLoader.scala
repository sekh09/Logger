package com.dnb.platform.ingestion.util

import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.dnb.platform.ingestion.exception.LoggerException

object PropertyFileLoader {

  /**
   * Load properties file based on profile
   */
  def loadPropertiesFile() {
    var prop = new Properties();
    var resourceAsStream = getClass().getClassLoader().getResourceAsStream("config.properties");
    try {
      prop.load(resourceAsStream);
      for (propName <- prop.stringPropertyNames()) {
        System.setProperty(propName, prop.getProperty(propName))
      }
    } catch {
      case e: Exception => {
        LoggingHolder.LOG.error("Error while loading properties file: " + e.getMessage)
        throw new LoggerException(e.getMessage);
      }
    }
  }
}