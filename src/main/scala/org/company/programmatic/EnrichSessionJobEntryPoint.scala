package org.company.programmatic

import org.apache.spark.sql.DataFrame
import org.company._
import org.company.programmatic.reader.DataReader
import AttributesNamesRegistry.category
import org.company.programmatic.session.SessionDataProcessor

object EnrichSessionJobEntryPoint extends App {

  override def main(args: Array[String]): Unit = {
    runEnrichSessionJob.sort(category).show(100)
  }

  def runEnrichSessionJob: DataFrame = {
    val data: DataFrame = DataReader.readData(dataFilePath)
    SessionDataProcessor.enrichBySession(data, sessionDurationThresholdInSeconds)
  }
}
