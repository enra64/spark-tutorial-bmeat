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
      .groupBy("_2")
      .agg(array_distinct(flatten(collect_set("collect_set(_1)"))))
      .select("array_distinct(flatten(collect_set(collect_set(_1))))")
      .flatMap(row => {
        val list = row.getAs[Seq[String]](0)
        list.map(item => {
          (item, list.filter(item2 => item2.toString != item.toString))
        })
        })
      .rdd
      .reduceByKey((first, second) => first.intersect(second))
      .toDF()
      .filter(row => row.getAs[Seq[String]](1).nonEmpty)
      .sort(asc("_1"))
      .foreach(row => println(row.getAs[String](0) + " < " + row.getAs[List[String]](1).mkString(",")))
  }
}
