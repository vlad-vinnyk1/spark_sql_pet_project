package org.company.sql

import org.company.sql.reader.SqlDataReader
import org.company.sql.session.SqlSessionDataProcessor
import org.company._

object EnrichSessionJobEntryPoint extends App {
  override def main(args: Array[String]): Unit = {
    runEnrichSessionJob.show(100)
  }

  private def runEnrichSessionJob = {
    SqlDataReader.readData(dataFilePath)
    SqlSessionDataProcessor.enrichBySession(sessionDurationThresholdInSeconds)
  }
}