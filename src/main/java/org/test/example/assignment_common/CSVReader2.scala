package org.test.example.assignment_common
import java.util.Properties

import org.apache.spark.sql.SparkSession


object CSVReader2 {
  def main(args: Array[String]) {
    val sc = SparkSession
      .builder()
      .appName("Spark CSV Reader")
      .config("spark.master", "local")
      .getOrCreate()
      /*if (args.length < 6) {
      System.err.println("Usage: Travel Agency Data <booking file name> " +
        "<hotel file name> <customer file name> <metricA file name> <metricB file name> <metricC file name>")
      System.exit(1)
    }*/
    val ReadBookingDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load("D:/Study/scala/assignment-common/spark-warehouse/emp.csv")
      
      import sc.implicits._
      ReadBookingDF.createOrReplaceTempView("tabA")
      val counts = sc.sql("select tabA.name,(YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(NOW(),'dd-MM-yyyy') AS TIMESTAMP)))-YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(tabA.dob,'dd-MM-yyyy') AS TIMESTAMP)))) as Age"
                           +""
                           +" from tabA group by tabA.name,Age")
      counts.coalesce(1).write.option("header", "true").option("sep", ",").csv("D:/Study/scala/assignment-common/spark-warehouse/counts.csv")
     /* val ReadHotelDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load("D:/Study/scala/assignment-common/spark-warehouse/depart.csv")
      
      ReadHotelDF.createOrReplaceTempView("tabB")
      
      val ReadCustomerDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load("D:/Study/scala/assignment-common/spark-warehouse/depart.csv")
      
      ReadCustomerDF.createOrReplaceTempView("tabC")
      
      val counts=sc.sql("select tabA.Client Id,tabC.First Name,tabC.Last Name,tabC.gender,tabC.age,tabB.country as HotelCountry,datediff(TO_DATE(CAST(UNIX_TIMESTAMP(tabA.enddate,'dd-MM-yyyy') AS TIMESTAMP)),"+
                        "TO_DATE(CAST(UNIX_TIMESTAMP(tabA.startdate,'dd-MM-yyyy') AS TIMESTAMP))) as Interval"+
                        "from tabA,tabB,tabC where tabA.Hotel Id=tabB.Hotel Id and tabA.Client Id=tabC.Client Id")
      
      
      val joinDF = sc.sql("select tabB.depart,tabA.depart from tabA,tabB where tabA.depart=tabB.name")
      println("Check====>"+joinDF.show())*/
  }
  
}