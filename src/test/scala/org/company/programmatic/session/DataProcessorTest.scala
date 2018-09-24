package org.company.programmatic.session

import org.apache.spark.sql.Row
import org.company.TestDataProvider._
import org.scalatest.{Matchers, WordSpec}

class DataProcessorTest extends WordSpec with Matchers {

  "DataProcessor Object" should {
    val sessionDurationThreshold = 299
    val sessionStart = 5
    val sessionEnd = 6
    val sessionId = 7

    "should enrich only by one session with same because of time range < 5 min" in {
      val result = SessionDataProcessor
        .enrichBySession(getProductDfWithOneSession, sessionDurationThreshold)
        .collect()

      val sessionData = extractSessionData(result)

      sessionData.forall(_ == sessionData.head) shouldEqual true
      assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(result, sessionData)
    }

    "should enrich only by two session" in {
      val result = SessionDataProcessor
        .enrichBySession(getProductDfWithTwoSessions, sessionDurationThreshold)
        .collect()

      val sessionDataResult = extractSessionData(result)

      sessionDataResult.forall(_ == sessionDataResult.head) shouldEqual false

      val idToData = result.groupBy(row => row.get(sessionId))
      idToData.size shouldEqual 3

      val events = idToData.values.toList
        .map(_.map(r =>
          (r.getString(0), r.getString(1), r.getString(2))))

      events.find(_.exists(_._2 == "book1")).get.length shouldEqual 3
      events.find(_.exists(_._2 == "book2")).get.length shouldEqual 2
      events.find(_.exists(item => item._2 == "book3")).get.length shouldEqual 2
      events.find(_.exists(_._2 == "book3")).get.length shouldEqual 2

      idToData.values.map(extractSessionData).foreach(
        sessionData => sessionData.forall(_ == sessionData.head) shouldEqual true)
      idToData.values.foreach(
        session => assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(session, extractSessionData(session)))
    }

    "should return session with different ids" in {
      val result = SessionDataProcessor
        .enrichBySession(getProductDfWhereSessionWithDifferentIds, sessionDurationThreshold)
        .collect()
      result.length shouldEqual 20
      result.groupBy(row => row.get(sessionId)).size shouldEqual 20
    }

    def assertThatFirstTimeAndLastTimeCorrespondToSessionStartAndEnd(result: Array[Row], sessionDataResult: Array[(Any, Any, Any)]) = {
      val firstEventTime = result.head.get(3)
      val lastEventTime = result.last.get(3)
      sessionDataResult.forall(sess => sess._2 == firstEventTime && sess._3 == lastEventTime) shouldEqual true
    }

    def extractSessionData(result: Array[Row]) = {
      result.map(r => (r.get(sessionId), r.get(sessionStart), r.get(sessionEnd)))
    }
  }
}