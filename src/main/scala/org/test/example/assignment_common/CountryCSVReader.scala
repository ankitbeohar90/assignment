package org.test.example.assignment_common
/**
 * @author Ankit Beohar
 * 
 * This is to read country data and return dataframe.
 * @param args(1) from Runner use here
 * @return countryDf 
 * 
 * */
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


object CountryCSVReader {
  val logger = LoggerFactory.getLogger(CountryCSVReader.getClass)    
    
  def readData(args: String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()
    
      val ReadHotelDF = sc.read.format("csv")
      .option("sep", "|")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("dateFormat","dd-MM-yyyy")
      .load(args)
      
      ReadHotelDF.createOrReplaceTempView("countryTab")
      
      val countryDf = sc.sql("select * from countryTab")
      return countryDf    
      
  }
 
}