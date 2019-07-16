package com.spark.dell.logtrends

import sys.process._
import java.net.URL
import java.io._

object LogFileDownloader {
  
  // Method to download the log file and save in the specified location
  def fileDownloader(url: String, fileName: String) = {
    val logFile = new File(fileName)
    val exists = logFile.exists()
    if(!exists) new URL(url) #> new File(fileName) !!
    }
  
  def main(arg: Array[String]) {
    
    
    try
    {
        // Calling fileDownloader method to download the log file
        fileDownloader("ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz", "logs.gz")
              
        // reading the trend limit number from the spark-submit command
        val trendLimit = arg(0).toInt
              
        // Calling calculateTrends method to calculate the top-n most frequent visitors and urls for each day of the trace
        TrendCalculator.calculateTrends(trendLimit)
    }
    
    catch {
            case ex: FileNotFoundException => println("Write to logs")
            case unknown: Exception => println("Write to logs")

          }
    

  
  
}
}