package org.company.programmatic

import org.company._
import org.company.programmatic.reader.DataReader
import org.company.programmatic.statistics.StatisticsDataProcessor

object TopTenProductsPerCategoryJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    calculateTopTenProducts.show(100)
  }

  private def calculateTopTenProducts = {
    val readData = DataReader.readData(dataFilePath)
    StatisticsDataProcessor.calculateTopTenProductsPerCategory(readData)
  }
}
