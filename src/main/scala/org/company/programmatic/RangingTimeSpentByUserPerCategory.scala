package org.company.programmatic

import org.apache.spark.sql.DataFrame
import org.company._
import org.company.programmatic.reader.DataReader
import org.company.programmatic.session.SessionDataProcessor
import org.company.programmatic.statistics.StatisticsDataProcessor

object RangingTimeSpentByUserPerCategory extends App {
  override def main(args: Array[String]): Unit = {
    val data: DataFrame = DataReader.readData(dataFilePath)
    val enrichedBySession = SessionDataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds)
    val userRanged = StatisticsDataProcessor.calculateUsersByTimeSpentPerCategory(enrichedBySession)

    userRanged.show()
  }

}