package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    inputs.foreach(input => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(input)
      .printSchema())

  }
}
