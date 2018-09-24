package org

package object company {

  implicit class RichDouble(value: Double) {
    def toSec: Double = value * 60
  }

  val dataFilePath: String = "/data.csv"

  val sessionDurationThresholdInSeconds: Int = 299

}
