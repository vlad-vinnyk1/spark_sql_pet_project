package org.company.sql

import org.company._
import org.company.sql.reader.SqlDataReader
import org.company.sql.reader.Tables.productsEnrichedBySessionTable
import org.company.sql.session.SqlSessionDataProcessor
import org.company.sql.statistics.SqlStatisticsDataProcessor

object RangingTimeSpentByUserPerCategory extends App {
  override def main(args: Array[String]): Unit = {
    SqlDataReader.readData(dataFilePath)
    SqlSessionDataProcessor.enrichBySession(sessionDurationThresholdInSeconds)
      .createOrReplaceTempView(productsEnrichedBySessionTable)
    val userRanged = SqlStatisticsDataProcessor.calculateUsersByTimeSpentPerCategory()

    userRanged.show()
  }

}