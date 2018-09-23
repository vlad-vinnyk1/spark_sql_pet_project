package org.company.sql.session

import org.apache.spark.sql.Row
import org.apache.spark.sql.Row._
import org.company.SparkApplicationInitializer
import org.company.programmatic.reader.DataReader
import org.company.sql.reader.Tables
import org.scalatest.{Matchers, WordSpec}

class SqlDataProcessorTest extends WordSpec with Matchers {

  {
    "DataProcessor Object" should {
      val sparkSession = SparkApplicationInitializer.sparkSession

      val sessionDurationThreshold = 299
      val sessionStart = 5
      val sessionEnd = 6
      val sessionId = 7

      "should enrich only by one session with same because of time range < 5 min" in {
        val row = List(
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:01:40", "like"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:01:50", "check status")
        ).map(fromSeq)
        val rdd = sparkSession.sparkContext.makeRDD(row)

        val df = sparkSession.createDataFrame(rdd, DataReader.schema)
        df.createOrReplaceTempView(Tables.productsTable)
        val result = SqlSessionDataProcessor.enrichBySession(sessionDurationThreshold).collect()
        val sessionData = extractSessionRelatedData(result)

        sessionData.forall(_ == sessionData.head) shouldEqual true
        assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(result, sessionData)
      }

      "should enrich only by two session" in {
        val rows = List(
          Seq("books", "book1", "user 100", "2018-03-01 12:00:02", "view description"),
          Seq("books", "book1", "user 100", "2018-03-01 12:01:40", "like"),
          Seq("books", "book1", "user 100", "2018-03-01 12:01:50", "check status"),

          Seq("books", "book2", "user 100", "2018-03-01 12:06:50", "like"),
          Seq("books", "book2", "user 100", "2018-03-01 12:07:50", "view description"),

          Seq("mobile phones", "book3", "user 100", "2018-03-01 12:06:50", "like"),
          Seq("mobile phones", "book3", "user 100", "2018-03-01 12:07:50", "view description")
        ).map(fromSeq)

        val rdd = sparkSession.sparkContext.makeRDD(rows)

        val df = sparkSession.createDataFrame(rdd, DataReader.schema)
        df.createOrReplaceTempView(Tables.productsTable)
        val result = SqlSessionDataProcessor.enrichBySession(sessionDurationThreshold).collect()

        val sessionDataResult = extractSessionRelatedData(result)
        val idToData = result.groupBy(row => row.get(sessionId))
        val events = idToData.values.toList.map(_.map(r => (r.getString(0), r.getString(1), r.getString(2))))

        sessionDataResult.forall(_ == sessionDataResult.head) shouldEqual false
        idToData.size shouldEqual 3
        events.find(_.exists(_._2 == "book1")).get.length shouldEqual 3
        events.find(_.exists(_._2 == "book2")).get.length shouldEqual 2
        events.find(_.exists(_._2 == "book3")).get.length shouldEqual 2

        idToData.values.map(extractSessionRelatedData).foreach(
          sessionData => sessionData.forall(_ == sessionData.head) shouldEqual true)
        idToData.values.foreach(
          session => assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(session, extractSessionRelatedData(session)))
      }

      "should return session with different ids" in {
        val rows = List(
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:05:40", "like"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:10:50", "check status"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:15:50", "like"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:20:50", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:25:50", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:30:50", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:35:50", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:40:50", "view description"),
          Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:45:51", "view description"),

          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:05:40", "like"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:10:50", "check status"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:15:50", "like"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:20:50", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:25:50", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:30:50", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:35:50", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:40:50", "view description"),
          Seq("mobile phones", "Scala for Dummies", "user 100", "2018-03-01 12:45:51", "view description")
        ).map(fromSeq)

        val rdd = sparkSession.sparkContext.makeRDD(rows)

        val df = sparkSession.createDataFrame(rdd, DataReader.schema)
        df.createOrReplaceTempView(Tables.productsTable)
        val result = SqlSessionDataProcessor.enrichBySession(sessionDurationThreshold).collect()
        result.length shouldEqual 20
        result.groupBy(row => row.get(sessionId)).size shouldEqual 20
      }

      def assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(result: Array[Row], sessionDataResult: Array[(Any, Any, Any)]) = {
        val firstEventTime = result.head.get(3)
        val lastEventTime = result.last.get(3)
        sessionDataResult.forall(sess => sess._2 == firstEventTime && sess._3 == lastEventTime) shouldEqual true
      }

      def extractSessionRelatedData(result: Array[Row]) = {
        result.map(r => (r.get(sessionId), r.get(sessionStart), r.get(sessionEnd)))
      }
    }
  }
}