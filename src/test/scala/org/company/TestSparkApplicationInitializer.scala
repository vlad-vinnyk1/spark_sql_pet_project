package org.company

import org.apache.spark.sql.SparkSession

object TestSparkApplicationInitializer {
  private lazy val session = SparkSession
    .builder()
    .appName("TestSparkApp")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  def sparkSession: SparkSession = session
}
