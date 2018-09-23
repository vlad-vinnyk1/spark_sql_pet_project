package org.company.sql

import org.company.sql.reader.SqlDataReader
import org.company.sql.reader.Tables.productsEnrichedBySessionTable
import org.company.sql.session.SqlSessionDataProcessor
import org.company.sql.statistics.SqlStatisticsDataProcessor

object RangingTimeSpentByUserPerCategory extends App {
  override def main(args: Array[String]): Unit = {
    SqlDataReader.readData("/data.csv")
    SqlSessionDataProcessor.enrichBySession(299)
      .createOrReplaceTempView(productsEnrichedBySessionTable)
    val userRanged = SqlStatisticsDataProcessor.calculateUsersByTimeSpentPerCategory()

    userRanged.show()
  }

}