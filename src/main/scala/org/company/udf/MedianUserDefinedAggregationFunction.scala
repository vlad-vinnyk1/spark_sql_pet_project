package org.company.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

class MedianUserDefinedAggregationFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(
    StructField("sessionDuration", LongType)
  ))

  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("items", ArrayType(LongType, containsNull = false))
    ))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Nil
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =
      input.getAs[Long](0) +: buffer.getAs[Seq[Long]](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =
      buffer2.getAs[Seq[Long]](0) +: buffer1.getAs[Seq[Long]](0)
  }

  override def evaluate(buffer: Row): Any = {
    val values = buffer
      .getAs[Seq[mutable.WrappedArray[Long]]](0)
      .toVector.flatten
      .map(_.toDouble)
    Utils.medianCalculator(values)
  }

  object Utils {
    def medianCalculator(seq: Seq[Double]): Double = {
      val sortedSeq = seq.sortWith(_ < _)

      if (sortedSeq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)
      else {
        val (up, down) = sortedSeq.splitAt(sortedSeq.size / 2)
        (up.last + down.head) / 2
      }
    }
  }

}