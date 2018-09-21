package org.company

import org.apache.spark.sql.DataFrame
import org.company.session.{DataProcessor, DataReader}

object EnrichSessionJobEntryPoint extends App {

  override def main(args: Array[String]): Unit = {
    runEnrichSessionJob.show()
  }

  def runEnrichSessionJob: DataFrame = {
    val data: DataFrame = DataReader.readData("/data.csv")
    DataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds = 299)
  }
}