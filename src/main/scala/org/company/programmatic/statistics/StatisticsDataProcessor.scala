package org.company.programmatic.statistics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.company.AttributesNamesRegistry._
import org.company.udf.EffisientSessionIdEvaluatorAggregateWindowFunction.calculateSession
import org.company.udf.MedianUserDefinedAggregationFunction

object StatisticsDataProcessor {
  private val rank = "rank"

  def calculateMedianPerCategory(enrichedBySession: DataFrame): DataFrame = {
    val median = new MedianUserDefinedAggregationFunction
    val window = Window.partitionBy(category, sessionId).orderBy(sessionDuration)
    val sessionDurationCol = unix_timestamp(col(sessionEndTime)) - unix_timestamp(col(sessionStartTime))
    val sessionWithMean = enrichedBySession
      .withColumn(sessionDuration, sessionDurationCol)
      .withColumn(rank, row_number().over(window)).where(col(rank) === lit(1)).drop(rank)
      .select(category, sessionId, sessionDuration)
    sessionWithMean.groupBy(category).agg(median(col(sessionDuration)).as("median"))
  }

  def calculateUsersByTimeSpentPerCategory(enrichedBySession: DataFrame): DataFrame = {
    val categorySessionUserWindow = Window
      .partitionBy(category, sessionId, userId)
      .orderBy(sessionDuration)

    def toRange(userDurationOnCategory: Column): Column = {
      when(userDurationOnCategory < 60, lit("< 1"))
        .otherwise(
          when(userDurationOnCategory > 300, lit("> 5"))
            .otherwise(lit("1 to 5"))
        )
    }

    val userDurationOnCategory = "userDurationOnCategory"

    val sessionDurationCol = unix_timestamp(col(sessionEndTime)) - unix_timestamp(col(sessionStartTime))
    val withSessionDuration = enrichedBySession
      .withColumn(sessionDuration, sessionDurationCol)
      .withColumn(rank, row_number().over(categorySessionUserWindow)).where(col(rank) === lit(1)).drop(rank)
      .groupBy(category, userId)
      .agg(sum(col(sessionDuration)).as(userDurationOnCategory))

    val timeSpent = "timeSpent"
    withSessionDuration
      .withColumn(timeSpent, toRange(col(userDurationOnCategory)))
      .sort(category, userId)
      .drop(userDurationOnCategory)
  }

  def calculateTopTenProductsPerCategory(readData: DataFrame): DataFrame = {
    val sessionDurationCol = unix_timestamp(col(sessionEndTime)) - unix_timestamp(col(sessionStartTime))
    val userWindow = Window.partitionBy(userId).orderBy(eventTime)
    val sessionUserWindow = Window.partitionBy(category, product, userId, sessionTemp)
    val orderedSessionWindow = Window
      .partitionBy(userId, sessionTemp)
      .orderBy(eventTime)
    val md5SessionIdCalc = md5(concat(col(userId), col(sessionStartTime), col(sessionEndTime)))
    val session = Window.partitionBy(sessionId).orderBy(eventTime)

    val sessionCol = coalesce(
      col(product) =!= lag(col(product), 1).over(userWindow),
      lit(false)).cast("bigint")

    val withSession = readData
      .withColumn(eventTimeInSecondsTemp, unix_timestamp(col(eventTime)))
      .withColumn(sessionTemp, sum(sessionCol).over(userWindow))

    val withSessionDuration = withSession
      .withColumn(sessionStartTime, min(col(eventTime)).over(sessionUserWindow))
      .withColumn(sessionEndTime, max(col(eventTime)).over(sessionUserWindow))
      .withColumn(sessionId, calculateSession(md5SessionIdCalc).over(orderedSessionWindow))
      .withColumn(sessionDuration, sessionDurationCol)
      .withColumn(rank, row_number().over(session)).where(col(rank) === lit(1)).drop(rank)
      .drop(sessionTemp).drop(eventTimeInSecondsTemp).drop(eventTime)

    val categoryProductWindow = Window.partitionBy(category).orderBy(col(sessionDuration).desc)

    val sumOfDurations = withSessionDuration
      .groupBy(category, product)
      .agg(sum(sessionDuration).as(sessionDuration))

    sumOfDurations
      .withColumn(rank, dense_rank().over(categoryProductWindow))
      .where(col(rank) <= 10)
      .drop(rank)
      .drop(sessionDuration)
      .sort(col(category), col(sessionDuration).desc)
  }
}