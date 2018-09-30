package org.company.programmatic.session

import org.apache.commons.lang3.math.NumberUtils.INTEGER_ZERO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{md5, _}
import org.company.udf.LazySessionIdEvalAggregateWindowFunction.calculateSession

object SessionDataProcessor {
  def enrichBySession(data: DataFrame, sessionDurationThresholdInSeconds: Int): DataFrame = {
    import org.company.AttributesNamesRegistry._
    //    val sessionDurationCol = unix_timestamp(col(sessionEndTime)) - unix_timestamp(col(sessionStartTime))
    val categoryWindow = Window.partitionBy(category, userId).orderBy(eventTime)
    val categoryAndSessionWindow = Window.partitionBy(category, userId, sessionTemp)
    val orderedCategoryAndSessionWindow = Window
      .partitionBy(category, userId, sessionTemp)
      .orderBy(sessionStartTime, sessionEndTime)
    val md5SessionIdCalc = md5(concat(col(category), col(userId), col(sessionStartTime), col(sessionEndTime)))

    val isLagEventTimeInSessionRangeCol = (
      coalesce(col(eventTimeInSecondsTemp) - lag(col(eventTimeInSecondsTemp), 1).over(categoryWindow), lit(INTEGER_ZERO)
      ) > sessionDurationThresholdInSeconds
      ).cast("bigint")

    val withSession = data
      .withColumn(eventTimeInSecondsTemp, unix_timestamp(col(eventTime)))
      .withColumn(sessionTemp, sum(isLagEventTimeInSessionRangeCol).over(categoryWindow))
    withSession
      .withColumn(sessionStartTime, min(col(eventTime)).over(categoryAndSessionWindow))
      .withColumn(sessionEndTime, max(col(eventTime)).over(categoryAndSessionWindow))
      .withColumn(sessionId, calculateSession(md5SessionIdCalc).over(orderedCategoryAndSessionWindow))
      //      .withColumn("sessionDuration", sessionDurationCol)
      .drop(col(sessionTemp)).drop(eventTimeInSecondsTemp)
  }
}