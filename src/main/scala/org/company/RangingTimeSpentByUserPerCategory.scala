package org.company

import org.apache.spark.sql.DataFrame
import org.company.session.SessionDataProcessor
import org.company.statistics.StatisticsDataProcessor

object RangingTimeSpentByUserPerCategory extends App {
  override def main(args: Array[String]): Unit = {
    val data: DataFrame = DataReader.readData("/data.csv")
    val enrichedBySession = SessionDataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds = 299)
    val userRanged = StatisticsDataProcessor.calculateUsersByTimeSpentPerCategory(enrichedBySession)

    userRanged.show()
  }

}