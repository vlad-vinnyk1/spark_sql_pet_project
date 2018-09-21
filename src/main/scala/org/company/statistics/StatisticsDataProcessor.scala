package org.company.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, unix_timestamp}
import org.company.udf.MedianUserDefinedAggregationFunction

object StatisticsDataProcessor {
  def calculateMedianPerCategory(enrichedBySession: DataFrame): DataFrame = {
    val median = new MedianUserDefinedAggregationFunction
    val window = Window.partitionBy("category", "sessionId").orderBy("sessionDuration")
    val sessionDurationCol = unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
    val sessionWithMean = enrichedBySession
      .withColumn("sessionDuration", sessionDurationCol)
      .withColumn("rn", row_number().over(window)).where(col("rn") === lit(1)).drop("rn")
      .select("category", "sessionId", "sessionDuration")
    sessionWithMean.groupBy("category").agg(median(col("category"), col("sessionId"), col("sessionDuration")))
  }
}
