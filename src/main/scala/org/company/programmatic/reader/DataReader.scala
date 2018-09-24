package org.company.programmatic.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.company.SparkApplicationInitializer
import org.company.AttributesNamesRegistry._

object DataReader {
  def readData(filePath: String): DataFrame = {
    SparkApplicationInitializer
      .sparkSession
      .read
      .schema(schema)
      .option("header", "true")
      .csv(getClass.getResource(filePath).toString)
      .toDF()
  }

  val schema: StructType = {
    StructType(Array(
      StructField(category, StringType),
      StructField(product, StringType),
      StructField(userId, StringType),
      StructField(eventTime, StringType),
      StructField(eventType, StringType)
    ))
  }

}
