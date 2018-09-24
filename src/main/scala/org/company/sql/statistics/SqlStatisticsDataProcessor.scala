package org.company.sql.statistics

import org.apache.spark.sql.DataFrame
import org.company.AttributesNamesRegistry._
import org.company._
import org.company.sql.reader.Tables.{productsEnrichedBySessionTable, _}

object SqlStatisticsDataProcessor {
  private val rank = "rank"

  private val sparkSession = SparkApplicationInitializer.sparkSession

  private val enrichedBySessionAndLimitByFirstRank: String = {
    val enrichedBySessionDuration =
      s"""
         |SELECT $allBaseColumns, $sessionId,
         |unix_timestamp($sessionEndTime) - unix_timestamp($sessionStartTime) as $sessionDuration
         |FROM ($productsEnrichedBySessionTable)
      """.stripMargin

    val enrichedByRank =
      s"""
         |SELECT $category, $sessionId, $sessionDuration, $userId,
         |       ROW_NUMBER() OVER(
         |                         PARTITION BY $category, $sessionId
         |                         ORDER BY $sessionDuration) as $rank
         |FROM ($enrichedBySessionDuration)
      """.stripMargin

    val enrichedByRankWithOnlyFirst =
      s"""
         |SELECT *
         |FROM ($enrichedByRank)
         |WHERE $rank = 1
      """.stripMargin

    enrichedByRankWithOnlyFirst
  }

  def calculateMedianPerCategory(): DataFrame = {
    sparkSession.sql("""CREATE TEMPORARY FUNCTION median AS 'org.company.udf.MedianUserDefinedAggregationFunction'""")

    val result = sparkSession.sql(
      s"""
         |SELECT $category, median($sessionDuration) as median
         |FROM ($enrichedBySessionAndLimitByFirstRank)
         |GROUP BY $category
      """.stripMargin)

    sparkSession.sql("""DROP TEMPORARY FUNCTION median""")

    result
  }

  def calculateUsersByTimeSpentPerCategory(): DataFrame = {
    val userDurationOnCategory = "userDurationOnCategory"
    val ranges = { x: Double =>
      if (x < 60) "< 1"
      else if (x >= 60 && x <= 300) "1 to 5"
      else "> 5"
    }
    sparkSession.udf.register("ranges", ranges(_: Double): String)


    val withSessionDuration =
      s"""
         |SELECT $category, $userId, sum($sessionDuration) as $userDurationOnCategory
         |FROM ($enrichedBySessionAndLimitByFirstRank)
         |GROUP BY $category, $userId
       """.stripMargin

    val withRank =
      s"""
         |SELECT $category, $userId,
         |       ranges($userDurationOnCategory) as rank, $userDurationOnCategory
         |FROM ($withSessionDuration)
         |ORDER BY $category, $userId
       """.stripMargin
    sparkSession.sql(withRank)
  }

  def calculateTopTenProductsPerCategory(): DataFrame = {
    val userWindow = s"PARTITION BY $userId ORDER BY $eventTime"
    val categoryProductUserIdSessionTmpUserWindow = s"PARTITION BY $category, $product, $userId, $sessionTemp"
    val isLagProductEqualsToCurrent =
      s"""
         |CAST(COALESCE($product != LAG($product, 1) OVER($userWindow), false) as bigint)
      """.stripMargin

    val enrichedByEventTimeInSecondsAndSessionTmp =
      s"""
         |SELECT $allBaseColumns,  unix_timestamp($eventTime) as $eventTimeInSecondsTemp,
         |       SUM($isLagProductEqualsToCurrent ) OVER($userWindow) as $sessionTemp
         |FROM $productsTable
       """.stripMargin

    val enrichedBySessionStartAndEndTime =
      s"""
         |SELECT $allBaseColumns, $sessionTemp,
         |   MIN($eventTime) OVER($categoryProductUserIdSessionTmpUserWindow) AS $sessionStartTime,
         |   MAX($eventTime) OVER($categoryProductUserIdSessionTmpUserWindow) AS $sessionEndTime
         |FROM ($enrichedByEventTimeInSecondsAndSessionTmp)
      """.stripMargin

    val enrichedBySessionId =
      s"""
         |SELECT $allBaseColumns,
         |       unix_timestamp($sessionEndTime) - unix_timestamp($sessionStartTime) AS $sessionDuration,
         |       MD5(CONCAT($category, $userId, $sessionStartTime, $sessionEndTime)) AS $sessionId,
         |       $sessionStartTime, $sessionEndTime
         |FROM ($enrichedBySessionStartAndEndTime)
       """
        .stripMargin

    val withRank =
      s"""
         |SELECT $allBaseColumns, $sessionId, $sessionStartTime, $sessionEndTime, $sessionDuration,
         |              ROW_NUMBER() OVER(
         |                         PARTITION BY $category, $sessionId
         |                         ORDER BY $sessionDuration) as $rank
         | FROM ($enrichedBySessionId)
       """.stripMargin

    val withSessionDuration =
      s"""
         |SELECT $category, $product, $sessionDuration
         |FROM ($withRank)
         |WHERE $rank = 1
      """.stripMargin

    val categoryProductWindow = s"PARTITION BY $category ORDER BY $sessionDuration DESC"

    val sumOfDurations =
      s"""
         |SELECT $category, $product, SUM($sessionDuration) as $sessionDuration
         |FROM ($withSessionDuration)
         |GROUP BY $category, $product
       """.stripMargin

    val withProductRankInsideCategory =
      s"""
         |SELECT $category, $product, $sessionDuration, DENSE_RANK() OVER($categoryProductWindow) AS $rank
         |FROM ($sumOfDurations)
       """.stripMargin
    sparkSession.sql(
      s"""
         |SELECT $category, $product
         |FROM ($withProductRankInsideCategory)
         |WHERE $rank <= 10
         |ORDER BY $category, $sessionDuration DESC
       """.stripMargin)
  }

}