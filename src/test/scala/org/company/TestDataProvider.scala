package org.company

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row.fromSeq
import org.company.programmatic.TestUtils

object TestDataProvider {

  def getProductsDfWhereSessionDurationIsZero: DataFrame = {
    val row = List(
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "like"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereMedianCalculatedFromEvenNumbers: DataFrame = {
    val row = List(
      //20
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:22", "like"),
      //30
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:50:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:51:02", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereMedianCalculatedFromOddNumbers: DataFrame = {
    val row = List(
      //20
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:22", "like"),
      //30
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:40:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:41:02", "check status"),
      //10
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:55:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:55:42", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWithTwoCategories: DataFrame = {
    val row = List(
      //20
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:22", "like"),
      //30
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:40:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:41:02", "check status"),
      //10
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:55:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:55:42", "check status"),

      //5
      Seq("note books", "Scala for Dummies", "user 200", "2018-03-01 12:40:15", "check status"),
      Seq("note books", "Scala for Dummies", "user 200", "2018-03-01 12:40:20", "check status"),
      //10
      Seq("note books", "Scala for Dummies", "user 200", "2018-03-01 12:55:32", "check status"),
      Seq("note books", "Scala for Dummies", "user 200", "2018-03-01 12:55:42", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereTimeSpentLessThanOneMinute: DataFrame = {
    val row = List(
      //10
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:12", "like"),
      //5
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:50:32", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:50:37", "check status"),
      //15
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:15", "check status"),
      //29
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:50:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:50:29", "check status"),

      //29
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 12:55:00", "check status"),
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 12:55:29", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereTimeBetweenOneAndFiveMinutes: DataFrame = {
    val row = List(
      //40
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:42", "like"),
      //21
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:50:00", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:50:21", "check status"),
      //15
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:01:15", "check status"),
      //45
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:50:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:50:45", "check status"),

      //299
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 12:55:00", "check status"),
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 12:59: 59", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereTimeMoreThanFiveMinutes: DataFrame = {
    val row = List(
      //120
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:02:02", "like"),
      //299
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:03:00", "check status"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:07:59", "check status"),
      //299
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:04:59", "check status"),
      //2
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:05:00", "check status"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:05:02", "check status"),

      //299
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 14:55:00", "check status"),
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 14:59:59", "check status"),

      //2
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 15:00:00", "check status"),
      Seq("note books", "Mac Pro", "user 200", "2018-03-01 15:00:02", "check status")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }

  def getProductsDfWhereTwoCategoryWithMoreThanTenProducts: DataFrame = {
    val row = List(
      //Scala for Dummies 5 sec
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:07", "like"),

      //BigData for Dummies 5 sec
      Seq("books", "BigData for Dummies", "user 100", "2018-03-01 12:01:03", "view description"),
      Seq("books", "BigData for Dummies", "user 100", "2018-03-01 12:01:08", "like"),

      //Scala for Dummies 6 sec
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:02:03", "view description"),
      Seq("books", "Scala for Dummies", "user 100", "2018-03-01 12:02:09", "like"),

      //Scala for Dummies 6 sec
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:02", "view description"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:00:08", "like"),

      //BigData for Dummies 5 sec
      Seq("books", "BigData for Dummies", "user 200", "2018-03-01 12:01:03", "view description"),
      Seq("books", "BigData for Dummies", "user 200", "2018-03-01 12:01:08", "like"),

      //Scala for Dummies 8 sec
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:02:03", "view description"),
      Seq("books", "Scala for Dummies", "user 200", "2018-03-01 12:02:11", "like"),

      //JavaScript for Dummies 59 sec
      Seq("books", "JavaScript for Dummies", "user 200", "2018-03-01 12:03:03", "view description"),
      Seq("books", "JavaScript for Dummies", "user 200", "2018-03-01 12:04:02", "like"),

      //PHP in Action 2 sec
      Seq("books", "PHP in Action", "user 407", "2018-03-01 12:03:03", "view description"),
      Seq("books", "PHP in Action", "user 407", "2018-03-01 12:03:05", "like"),

      //ORM in Action 3 sec
      Seq("books", "ORM in Action", "user 406", "2018-03-01 12:03:03", "view description"),
      Seq("books", "ORM in Action", "user 406", "2018-03-01 12:03:06", "like"),

      //Romeo and Juliet 4 sec
      Seq("books", "Romeo and Juliet", "user 300", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Romeo and Juliet", "user 300", "2018-03-01 12:03:07", "like"),

      //Scala in Action 5 sec
      Seq("books", "Scala in Action", "user 400", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Scala in Action", "user 400", "2018-03-01 12:03:08", "like"),

      //Spring in Action 6 sec
      Seq("books", "Spring in Action", "user 401", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Spring in Action", "user 401", "2018-03-01 12:03:09", "like"),

      //Spark in Action 7 sec
      Seq("books", "Spark in Action", "user 402", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Spark in Action", "user 402", "2018-03-01 12:03:10", "like"),

      //Hadoop in Action 8 sec
      Seq("books", "Hadoop in Action", "user 403", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Hadoop in Action", "user 403", "2018-03-01 12:03:11", "like"),

      //Hive in Action 9 sec
      Seq("books", "Hive in Action", "user 404", "2018-03-01 12:03:03", "view description"),
      Seq("books", "Hive in Action", "user 404", "2018-03-01 12:03:12", "like"),

      //Bicycles: BMX 9 sec
      Seq("Bicycles", "BMX", "user 404", "2018-03-01 12:53:03", "view description"),
      Seq("Bicycles", "BMX", "user 404", "2018-03-01 12:53:12", "like")
    ).map(fromSeq)
    TestUtils.toDataFrame(row)
  }
}
