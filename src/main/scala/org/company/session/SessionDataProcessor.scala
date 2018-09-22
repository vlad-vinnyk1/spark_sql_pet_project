package org.company.session

import org.apache.commons.lang3.math.NumberUtils.{max => _, min => _, _}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SessionDataProcessor {
  def enrichBySession(data: DataFrame, sessionDurationThresholdInSeconds: Int): DataFrame = {
    import org.company.session.AttributesNamesRegistry._
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


  def rangeUsersByTimeSpentPerCategory(enrichedBySession: DataFrame): DataFrame = {
    val categorySessionUserWindow = Window
      .partitionBy("category", "sessionId", "userId")
      .orderBy("sessionDuration")

    val splits = Array(Double.NegativeInfinity, 1.toSec, 5.toSec, Double.PositiveInfinity)
    val ranges = functions.udf { x: Double =>
      x match {
        case 0 => "< 1"
        case 1 => "1 to 5"
        case 2 => "> 5"
      }
    }

    val bucketizer = new Bucketizer()
      .setInputCol("userDurationOnCategory")
      .setOutputCol("range")
      .setSplits(splits)

    val sessionDurationCol = unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
    val withSessionDuration = enrichedBySession
      .withColumn("sessionDuration", sessionDurationCol)
      .withColumn("rn", row_number().over(categorySessionUserWindow)).where(col("rn") === lit(1)).drop("rn")
      .groupBy("category", "userId")
      .agg(sum(col("sessionDuration")).as("userDurationOnCategory"))

    bucketizer.transform(withSessionDuration)
      .withColumn("timeSpent", ranges(col("range")))
      .drop("range")
      .sort("category", "userId")
      .drop("userDurationOnCategory")
  }

}