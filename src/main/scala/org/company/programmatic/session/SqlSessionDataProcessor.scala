package org.company.programmatic.session

import org.apache.commons.lang3.math.NumberUtils.INTEGER_ZERO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}

object SqlSessionDataProcessor {
  def enrichBySession(data: DataFrame, sessionDurationThresholdInSeconds: Int) = {


  }


  def enrichBySessionOld(data: DataFrame, sessionDurationThresholdInSeconds: Int): DataFrame = {
    import org.company.programmatic.session.AttributesNamesRegistry._
    val sessionDurationCol = unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
    val categoryWindow = Window.partitionBy(category, "userId").orderBy(eventTime)
    val categoryAndSessionWindow = Window.partitionBy(category, "userId", sessionTemp)

    val sessionCol = (
      coalesce(col(eventTimeInSecondsTemp) - lag(col(eventTimeInSecondsTemp), 1).over(categoryWindow), lit(INTEGER_ZERO)
      ) > sessionDurationThresholdInSeconds
      ).cast("bigint")

    val withSession = data
      .withColumn(eventTimeInSecondsTemp, unix_timestamp(col(eventTime)))
      .withColumn(sessionTemp, sum(sessionCol).over(categoryWindow))

    withSession
      .withColumn(sessionStartTime, min(col(eventTime)).over(categoryAndSessionWindow))
      .withColumn(sessionEndTime, max(col(eventTime)).over(categoryAndSessionWindow))
      .withColumn(sessionId, md5(concat(col(category), col(sessionStartTime), col(sessionEndTime))))
      .withColumn("sessionDuration", sessionDurationCol)
      .drop(col(sessionTemp)).drop(eventTimeInSecondsTemp)
  }

}