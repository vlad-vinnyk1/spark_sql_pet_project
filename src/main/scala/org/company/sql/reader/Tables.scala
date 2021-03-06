package org.company.sql.reader

import org.company.AttributesNamesRegistry._

object Tables {
  val productsTable = "products_csv"
  val productsEnrichedBySessionTable = "enrichedBySession"

  val allBaseColumns: String = Array(category, product, userId, eventTime, eventType).mkString(",")
}
