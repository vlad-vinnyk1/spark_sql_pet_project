package org.company.sql

import org.company.dataFilePath
import org.company.sql.reader.SqlDataReader
import org.company.sql.statistics.SqlStatisticsDataProcessor

object TopTenProductsPerCategoryJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    calculateTopTenProducts.show(100)
  }

  private def calculateTopTenProducts = {
    SqlDataReader.readData(dataFilePath)
    SqlStatisticsDataProcessor.calculateTopTenProductsPerCategory()
  }
}