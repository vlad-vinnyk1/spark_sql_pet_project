package org.company

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

  import org.company.session.AttributesNamesRegistry._

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
