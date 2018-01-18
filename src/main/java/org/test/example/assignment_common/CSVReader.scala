package org.test.example.assignment_common
import java.util.Properties

import org.apache.spark.sql.SparkSession


object CSVReader {
  def main(args: Array[String]) {
    val sc = SparkSession
      .builder()
      .appName("Spark CSV Reader")
      .config("spark.master", "local")
      .getOrCreate()
      if (args.length < 6) {
      System.err.println("Usage: Travel Agency Data <booking file name> " +
        "<hotel file name> <customer file name> <metricA file name> <metricB file name> <metricC file name>")
      System.exit(1)
    }
    val ReadBookingDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args(0))
      
      import sc.implicits._
      ReadBookingDF.createOrReplaceTempView("tabA")
      
      val ReadHotelDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args(1))
      
      ReadHotelDF.createOrReplaceTempView("tabB")
      
      val ReadCustomerDF = sc.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args(2))
      
      ReadCustomerDF.createOrReplaceTempView("tabC")
      
      val metricA=sc.sql("select tabA.Client Id,tabC.First Name,tabC.Last Name,tabC.gender,(YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(NOW(),'dd-MM-yyyy') AS TIMESTAMP)))-YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(tabC.age,'dd-MM-yyyy') AS TIMESTAMP)))) as Age "+
                        ",tabB.country as HotelCountry,datediff(TO_DATE(CAST(UNIX_TIMESTAMP(tabA.enddate,'dd-MM-yyyy') AS TIMESTAMP)),"+
                        " TO_DATE(CAST(UNIX_TIMESTAMP(tabA.startdate,'dd-MM-yyyy') AS TIMESTAMP))) as Interval"+
                        " from tabA,tabB,tabC where tabA.Hotel Id=tabB.Hotel Id and tabA.Client Id=tabC.Client Id group by "+
                        " tabA.Client Id,tabC.First Name,tabC.Last Name,tabC.gender,Age,Interval")
      
      
      val metricB = sc.sql("select tabA.Stay Duration,tabB.city,tabB.country from tabA,tabB where tabA.Hotel Id=tabB.Hotel Id")
      
      val metricC = sc.sql("select tabA.Stay Duration,tabA.gender,(YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(NOW(),'dd-MM-yyyy') AS TIMESTAMP)))-YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(tabC.age,'dd-MM-yyyy') AS TIMESTAMP)))) as Age  from tabA,tabC where tabA.Client Id=tabC.Client Id")
            
      metricA.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(3))
      metricB.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(4))
      metricC.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(5))
      
  }
  
}