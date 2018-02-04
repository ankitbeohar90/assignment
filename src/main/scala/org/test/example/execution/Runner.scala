package org.test.example.execution
/**
 * @author Ankit Beohar
 * 
 * This is a runner code and 
 * all child or dependent object call from here
 * 
 * @param args(0) = booking file, args(1) = country file, args(1) = hotel file
 *        args(2) = metricA outfile, args(3) = metricB outfile, args(4) =  metricC outfile
 * 
 * */
import org.apache.spark.sql
import org.slf4j.{ LoggerFactory, Marker, Logger => Underlying }
import org.test.example.common.PropertiesLoader
import org.test.example.assignment_common.BookingCSVReader
import org.test.example.assignment_common.CountryCSVReader
import org.test.example.assignment_common.HotelCSVReader
import org.test.example.assignment_common.SparkSessionLoader
import java.io.IOException

object Runner {
  val logger = LoggerFactory.getLogger(Runner.getClass)
  val sc = SparkSessionLoader.getSparkSession()
   def main(args: Array[String]) {
     try{
       //read all data one by one and get the DF
       val tabA = BookingCSVReader.readData(args(0))
       val tabB = CountryCSVReader.readData(args(1))
       val tabC = HotelCSVReader.readData(args(2))
       
       // read and run all metrics 
       val metricA=sc.sql(PropertiesLoader.properties.getProperty("sqlA"))
       val metricB = sc.sql(PropertiesLoader.properties.getProperty("sqlB"))
       val metricC = sc.sql(PropertiesLoader.properties.getProperty("sqlC"))
       
       // create outfile of all data
       metricA.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(3))
       metricB.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(4))
       metricC.coalesce(1).write.option("header", "true").option("sep", ",").csv(args(5))
       logger.debug("Process Done Check Data in Outfiles")
     }
     catch{
       case e: Exception => logger.error("Unable to run Scala Job"+e)
      return null
     }
     
   }
  // evaluate command line arguments and exit if any of it unavailable 
   def evaluateCommandLine(args: Array[String]){
   if (args.length < 6) {
     logger.error("Invalid number of parameters")
     System.err.println("Usage: Travel Agency Data <booking file name> " +
        "<hotel file name> <customer file name> <metricA file name> <metricB file name> <metricC file name>")
      System.exit(1)
    }
 }
   
}