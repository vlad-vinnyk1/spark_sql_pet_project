package org.company

import org.company.statistics.StatisticsDataProcessor

object TopTenProductsPerCategoryJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    calculateTopTenProducts.show(100)
  }

  private def calculateTopTenProducts = {
    val readData = DataReader.readData("/data.csv")
    StatisticsDataProcessor.calculateTopTenProductsPerCategory(readData)
  }
}