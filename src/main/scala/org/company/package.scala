package org

package object company {
  type Minutes = Double


  implicit class RichDouble(value: Minutes) {
    def toSec: Double = value * 60
  }

  val dataFilePath: String = "/data.csv"

  val sessionDurationThresholdInSeconds: Int = 299

}
