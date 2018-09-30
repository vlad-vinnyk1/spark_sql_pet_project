package org.company.programmatic

import org.apache.spark.sql.DataFrame
import org.company._
import org.company.programmatic.reader.DataReader
import org.company.programmatic.session.SessionDataProcessor
import org.company.programmatic.statistics.StatisticsDataProcessor

object MedianPerCategoryJobEntryPoint {
  def main(args: Array[String]): Unit = {
    medianPerCategory.show(100)
  }

  private def medianPerCategory: DataFrame = {
    val data: DataFrame = DataReader.readData(dataFilePath)
    val enrichedBySession = SessionDataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds)
    StatisticsDataProcessor.calculateMedianPerCategory(enrichedBySession)
  }
}
