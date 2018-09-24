name := "Scala_Spark_PeT"

version := "0.1"

scalaVersion := "2.11.6"
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4",

  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5",

  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.commons" % "commons-lang3" % "3.7"
)


