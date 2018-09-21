package org.company.session

import org.apache.commons.lang3.math.NumberUtils.{max => _, min => _, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DataProcessor {
  def enrichBySession(data: DataFrame, sessionDurationThresholdInSeconds: Int): DataFrame = {
    import org.company.session.AttributesNamesRegistry._

    val categoryWindow = Window.partitionBy(category).orderBy(eventTime)
    val categoryAndSessionWindow = Window.partitionBy(category, sessionTemp)

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
      .drop(col(sessionTemp)).drop(eventTimeInSecondsTemp)
  }
}