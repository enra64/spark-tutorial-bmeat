package de.hpi.spark_tutorial

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Sindy {


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val df: Unit = inputs.map(input => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(input))
      .map(
        df =>
          df.columns.foldLeft(df) { (acc, col) => acc.withColumn(col, df(col).cast("string")) }
            .flatMap(row => row.getValuesMap[String](row.schema.fieldNames))
            .groupBy("_2")
            .agg(collect_set("_1"))
      )
      .reduce(_.union(_))
      //.show(100, truncate = false)
      .groupBy("_2")
      .agg(array_distinct(flatten(collect_set("collect_set(_1)"))))
      .select("array_distinct(flatten(collect_set(collect_set(_1))))")
      //.flatMap(row => row.getList(0).stream().map(colName => ))
      .show(100, truncate = false)

  }
}
