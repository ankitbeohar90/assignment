package org.test.example.assignment_common
/**
 * @author Ankit Beohar
 * 
 * This is to read booking data and return dataframe.
 * @param args(0) from Runner use here
 * @return bookingDf 
 * 
 * */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.test.example.common.PropertiesLoader

import org.slf4j.{ LoggerFactory, Marker, Logger => Underlying }
import org.apache.spark.sql.DataFrameReader


object BookingCSVReader {
  val logger = LoggerFactory.getLogger(BookingCSVReader.getClass)    
    
  def readData(args: String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()
    
    val ReadBookingDF = sc.read.format("csv")
      .option("sep", "|")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args)
      
     ReadBookingDF.createOrReplaceTempView("bookingTab")
     
      val bookingDf=sc.sql("select * from bookingTab")
      return bookingDf 
  }
 
}