package de.hpi.spark_tutorial

import java.io.File

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object SimpleSpark extends App {
  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Lambda basics (for Scala)
    //------------------------------------------------------------------------------------------------------------------
//
//    //spark uses user defined functions to transform data, lets first look at how functions are defined in scala:
//    val smallListOfNumbers = List(1, 2, 3, 4, 5)
//
//    // A Scala map function from int to double
//    def squareAndAdd(i: Int): Double = {
//      i * 2 + 0.5
//    }
//    // A Scala map function defined in-line (without curly brackets)
//    def squareAndAdd2(i: Int): Double = i * 2 + 0.5
//    // A Scala map function inferring the return type
//    def squareAndAdd3(i: Int) = i * 2 + 0.5
//    // An anonymous Scala map function assigned to a variable
//    val squareAndAddFunction = (i: Int) => i * 2 + 0.5


    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("BMeat")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    //------------------------------------------------------------------------------------------------------------------
    // Longest Common Substring Search
    //------------------------------------------------------------------------------------------------------------------

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------
    val inputs = List("./TPCH/tpch_nation.csv", "./TPCH/tpch_region.csv", "./TPCH/tpch_supplier.csv", "./TPCH/tpch_part.csv", "./TPCH/tpch_orders.csv", "./TPCH/tpch_lineitem.csv", "./TPCH/tpch_customer.csv")
    //val inputs = List("./PAPENBROCK/artists.csv", "./PAPENBROCK/tracks.csv")
    time {Sindy.discoverINDs(inputs, spark)}
  }
}
