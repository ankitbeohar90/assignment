package org.test.example.assignment_common
/**
 * @author Ankit Beohar
 * 
 * This is to read hotel data and return dataframe.
 * @param args(2) from Runner use here
 * @return hotelDf 
 * 
 * */

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


object HotelCSVReader {
  val logger = LoggerFactory.getLogger(HotelCSVReader.getClass)    
    
  def readData(args:String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()
    
      val ReadCustomerDF = sc.read.format("csv")
      .option("sep", "|")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args)
      
      ReadCustomerDF.createOrReplaceTempView("hotelTab")
      
      val hotelDf = sc.sql("select * from hotelTab")
      return hotelDf 
  }
 
}