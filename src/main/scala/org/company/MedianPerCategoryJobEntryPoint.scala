package org.company

import org.apache.spark.sql.DataFrame
import org.company.session.SessionDataProcessor
import org.company.statistics.StatisticsDataProcessor

object MedianPerCategoryJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    medianPerCategory.show()
  }

  private def medianPerCategory: DataFrame = {
    val data: DataFrame = DataReader.readData("/data.csv")
    val enrichedBySession = SessionDataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds = 299)
    StatisticsDataProcessor.calculateMedianPerCategory(enrichedBySession)
  }
}