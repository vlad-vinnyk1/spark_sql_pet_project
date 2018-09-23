package org.company.sql

import org.apache.spark.sql.DataFrame
import org.company.sql.reader.SqlDataReader
import org.company.sql.reader.Tables._
import org.company.sql.session.SqlSessionDataProcessor
import org.company.sql.statistics.SqlStatisticsDataProcessor

object MedianPerCategoryJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    medianPerCategory.show()
  }

  private def medianPerCategory: DataFrame = {
    SqlDataReader.readData("/data.csv")
    SqlSessionDataProcessor.enrichBySession(299)
      .createOrReplaceTempView(productsEnrichedBySessionTable)

    SqlStatisticsDataProcessor.calculateMedianPerCategory()
  }
}