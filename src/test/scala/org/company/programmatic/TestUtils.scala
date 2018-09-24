package org.company.programmatic

import org.apache.spark.sql.{DataFrame, Row}
import org.company.SparkApplicationInitializer
import org.company.programmatic.reader.DataReader
import org.company.programmatic.session.SessionDataProcessor

object TestUtils {
  private val sparkSession = SparkApplicationInitializer.sparkSession
  private val sessionDurationThreshold = 299


  def toProductDataFrame(row: List[Row]): DataFrame = {
    val rdd = sparkSession.sparkContext.makeRDD(row)
    sparkSession.createDataFrame(rdd, DataReader.schema)
  }

  def toDataFrame(row: List[Row]): DataFrame = {
    val productsDataFrame = TestUtils.toProductDataFrame(row)
    SessionDataProcessor.enrichBySession(productsDataFrame, sessionDurationThreshold)
  }
}
