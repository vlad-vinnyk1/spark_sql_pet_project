package org.company

package object statistics {

  implicit class RichDouble(value: Double) {
    def toSec: Double = value * 60
  }

}
