package org.company

import org.apache.spark.sql.SparkSession

object SparkApplicationInitializer {
  private lazy val session = SparkSession
    .builder()
    .appName("SparkApp")
    .master("local[*]")
    .getOrCreate()

  def sparkSession: SparkSession = session
}
