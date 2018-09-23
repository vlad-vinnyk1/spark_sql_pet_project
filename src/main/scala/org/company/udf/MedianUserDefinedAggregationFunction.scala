package org.company.udf

import org.apache.commons.lang3.StringUtils.EMPTY
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MedianUserDefinedAggregationFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(
    StructField("sessionDuration", LongType)
  ))

  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("items", StringType)
    ))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = EMPTY
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =
      buffer.getAs[String](0) + "," +
        input.getAs[Double](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =
      buffer1.getAs[String](0) +
        buffer2.getAs[String](0)
  }

  override def evaluate(buffer: Row): Any = {
    val values = buffer.getString(0).split(",")
      .filter(str => EMPTY != str)
      .map(str => str.toDouble)
    Utils.medianCalculator(values)
  }

  object Utils {
    def medianCalculator(seq: Seq[Double]): Double = {
      val sortedSeq = seq.sortWith(_ < _)

      if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)
      else {
        val (up, down) = sortedSeq.splitAt(seq.size / 2)
        (up.last + down.head) / 2
      }
    }
  }

}
