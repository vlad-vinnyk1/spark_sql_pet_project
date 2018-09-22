package org.company.programmatic

import org.apache.spark.sql.DataFrame
import org.company.programmatic.reader.DataReader
import org.company.programmatic.session.SessionDataProcessor
import org.company.programmatic.statistics.StatisticsDataProcessor

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
