package org.company.sql.session

import org.apache.spark.sql.DataFrame
import org.company.SparkApplicationInitializer
import org.company.programmatic.session.AttributesNamesRegistry._
import org.company.sql.session.Tables._

object SqlSessionDataProcessor {
  def enrichBySession(sessionDurationThresholdInSeconds: Int): DataFrame = {
    val sparkSession = SparkApplicationInitializer.sparkSession
    val allBaseColumns = Array(category, product, userId, eventTime, eventType).mkString(",")
    val categoryAndUserWindow = s"PARTITION BY $category, $userId ORDER BY $eventTime"
    val categoryUserAndSessionWindow = s"PARTITION BY $category, $userId, $sessionTemp"

    val enrichedByEventTimeInSeconds =
      s"""
         |SELECT $allBaseColumns,
         |                UNIX_TIMESTAMP($eventTime) as $eventTimeInSecondsTemp
         |FROM $productsTable
       """.stripMargin
    val isLagEventTimeInSessionRange =
      s"""
         |CAST(
         |      COALESCE($eventTimeInSecondsTemp - LAG($eventTimeInSecondsTemp, 1) OVER($categoryAndUserWindow), 0) >
         |      $sessionDurationThresholdInSeconds AS bigint
         |)
      """.stripMargin

    val enrichedBySessionTmp =
      s"""
         |SELECT $allBaseColumns,
         |    SUM($isLagEventTimeInSessionRange) OVER($categoryAndUserWindow) AS $sessionTemp
         |FROM ($enrichedByEventTimeInSeconds)
      """.stripMargin

    val enrichedBySessionStartAndEndTime =
      s"""
         |SELECT $allBaseColumns, $sessionTemp,
         |   MIN($eventTime) OVER($categoryUserAndSessionWindow) AS $sessionStartTime,
         |   MAX($eventTime) OVER($categoryUserAndSessionWindow) AS $sessionEndTime
         |FROM ($enrichedBySessionTmp)
      """.stripMargin

    sparkSession.sql(
      s"""
         |SELECT $allBaseColumns, $sessionStartTime, $sessionEndTime,
         |    MD5(CONCAT($category, $userId, $sessionStartTime, $sessionEndTime)) AS $sessionId
         |FROM ($enrichedBySessionStartAndEndTime)
      """.stripMargin)
  }

}