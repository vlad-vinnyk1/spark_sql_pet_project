package org.company

package object session {
  implicit class RichDouble(value: Double) {
    def toSec: Double = value * 60
  }
}
