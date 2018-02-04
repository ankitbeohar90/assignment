package org.test.example.assignment_common
/**
 * @author Ankit Beohar
 * This is just a demo to describe how we can use structred streaming concept
 * of Spark 2.x
 * */
import java.util.Properties

import org.apache.spark.sql.SparkSession


object CSVStreamReader {
  def main(args: Array[String]) {
    val sc = SparkSession
      .builder()
      .appName("Spark CSV Reader")
      .config("spark.master", "local")
      .getOrCreate()
      import org.apache.spark.sql.types._
 val schemaA = new StructType().add("name","string").add("depart","string").add("startdate","string").add("enddate","string").add("dob","string")
  val schemaB = new StructType().add("name","string").add("depart","string")
    val ReadDFCsv = sc.readStream.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true").schema(schemaA)
      .load("D:/Study/scala/assignment-common/spark-warehouse/*")
     
   println("CHeck=====>"+ReadDFCsv.schema)
  }
  
}