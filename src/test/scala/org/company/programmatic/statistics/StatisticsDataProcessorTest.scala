package org.company.programmatic.statistics

import org.company.AttributesNamesRegistry.category
import org.company.TestDataProvider._
import org.scalatest.{Matchers, WordSpec}

class StatisticsDataProcessorTest extends WordSpec with Matchers {
  "StatisticsDataProcessor Object" should {

    //Median Tests
    "should calculate median when category has even number of items" in {
      val result = StatisticsDataProcessor.calculateMedianPerCategory(getProductsDfWhereMedianCalculatedFromEvenNumbers).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getDouble(1) shouldEqual 25.0
    }

    "should calculate median when category has odd number of items" in {
      val result = StatisticsDataProcessor.calculateMedianPerCategory(getProductsDfWhereMedianCalculatedFromOddNumbers).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getDouble(1) shouldEqual 20.0
    }

    "should calculate valid median for every category" in {
      val result = StatisticsDataProcessor.calculateMedianPerCategory(getProductsDfWithTwoCategories)
        .sort(category).collect()

      result(0).getString(0) shouldEqual "books"
      result(0).getDouble(1) shouldEqual 20.0
      
      result(1).getString(0) shouldEqual "note books"
      result(1).getDouble(1) shouldEqual 7.5
    }

    "should calculate median = 0 when session duration is 0" in {
      val result = StatisticsDataProcessor.calculateMedianPerCategory(getProductsDfWhereSessionDurationIsZero).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getDouble(1) shouldEqual 0.0
    }

    //Range by time spent Tests

    "should evaluate to two user spent less than one minute on categories" in {
      val result = StatisticsDataProcessor.calculateUsersByTimeSpentPerCategory(getProductsDfWhereTimeSpentLessThanOneMinute).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getString(1) shouldEqual "user 100"
      result(0).getString(2) shouldEqual "< 1"

      result(1).getString(0) shouldEqual "books"
      result(1).getString(1) shouldEqual "user 200"
      result(1).getString(2) shouldEqual "< 1"

      result(2).getString(0) shouldEqual "note books"
      result(2).getString(1) shouldEqual "user 200"
      result(2).getString(2) shouldEqual "< 1"
    }


    "should evaluate to two user spent between one minute and five minutes categories" in {
      val result = StatisticsDataProcessor.calculateUsersByTimeSpentPerCategory(getProductsDfWhereTimeBetweenOneAndFiveMinutes).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getString(1) shouldEqual "user 100"
      result(0).getString(2) shouldEqual "1 to 5"

      result(1).getString(0) shouldEqual "books"
      result(1).getString(1) shouldEqual "user 200"
      result(1).getString(2) shouldEqual "1 to 5"

      result(2).getString(0) shouldEqual "note books"
      result(2).getString(1) shouldEqual "user 200"
      result(2).getString(2) shouldEqual "1 to 5"
    }


    "should evaluate to two user spent more than five minutes category" in {
      val result = StatisticsDataProcessor.calculateUsersByTimeSpentPerCategory(getProductsDfWhereTimeMoreThanFiveMinutes).collect()
      result(0).getString(0) shouldEqual "books"
      result(0).getString(1) shouldEqual "user 100"
      result(0).getString(2) shouldEqual "> 5"

      result(1).getString(0) shouldEqual "books"
      result(1).getString(1) shouldEqual "user 200"
      result(1).getString(2) shouldEqual "> 5"

      result(2).getString(0) shouldEqual "note books"
      result(2).getString(1) shouldEqual "user 200"
      result(2).getString(2) shouldEqual "> 5"
    }

    "should result in top ten products per category" in {
      val result = StatisticsDataProcessor
        .calculateTopTenProductsPerCategory(getProductsDfWhereTwoCategoryWithMoreThanTenProducts)
        .collect()
        .map(row => (row.getString(0), row.getString(1)))
        .groupBy(_._1)

      val topTenBooks = result("books")
      val topTenBicycles = result("Bicycles")

      topTenBooks.length shouldEqual 10
      topTenBicycles.length shouldEqual 1

      //Assert Top Ten Books Rank Order
      topTenBooks(0)._1 -> topTenBooks(0)._2 shouldEqual "books" -> "JavaScript for Dummies"
      topTenBooks(1)._1 -> topTenBooks(1)._2 shouldEqual "books" -> "Scala for Dummies"
      topTenBooks(2)._1 -> topTenBooks(2)._2 shouldEqual "books" -> "BigData for Dummies"
      topTenBooks(3)._1 -> topTenBooks(3)._2 shouldEqual "books" -> "Hive in Action"
      topTenBooks(4)._1 -> topTenBooks(4)._2 shouldEqual "books" -> "Hadoop in Action"
      topTenBooks(5)._1 -> topTenBooks(5)._2 shouldEqual "books" -> "Spark in Action"
      topTenBooks(6)._1 -> topTenBooks(6)._2 shouldEqual "books" -> "Spring in Action"
      topTenBooks(7)._1 -> topTenBooks(7)._2 shouldEqual "books" -> "Scala in Action"
      topTenBooks(8)._1 -> topTenBooks(8)._2 shouldEqual "books" -> "Romeo and Juliet"
      topTenBooks(9)._1 -> topTenBooks(9)._2 shouldEqual "books" -> "ORM in Action"

      //Assert Top Ten Bicycles
      topTenBicycles(0)._1 -> topTenBicycles(0)._2 shouldEqual "Bicycles" -> "BMX"
      result
    }

  }

}