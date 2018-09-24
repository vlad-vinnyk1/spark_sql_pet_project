package org.company.sql

import org.company._
import org.company.sql.reader.SqlDataReader
import org.company.sql.session.SqlSessionDataProcessor

object EnrichSessionJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    runEnrichSessionJob.show(100)
  }

  private def runEnrichSessionJob = {
    SqlDataReader.readData(dataFilePath)
    SqlSessionDataProcessor.enrichBySession(sessionDurationThresholdInSeconds)
  }
}