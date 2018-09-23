package org.company.sql.reader

import org.company.SparkApplicationInitializer
import org.company.programmatic.session.AttributesNamesRegistry._
import org.company.sql.reader.Tables._

object SqlDataReader {
  def readData(filePath: String): Unit = {
    val path = getClass.getResource(filePath).toString
    val session = SparkApplicationInitializer
      .sparkSession
    session.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $productsTable (
         |$category STRING,
         |$product STRING,
         |$userId STRING,
         |$eventTime STRING,
         |$eventType STRING)
         |USING csv
         |OPTIONS (
         |inferSchema true,
         |header true,
         |path '$path')
        """.stripMargin
    )
  }

}