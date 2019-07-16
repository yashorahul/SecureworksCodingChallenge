package com.spark.dell.logtrends


import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


object TrendCalculator {

  // Method to match the logs based on the pattern
  def matchLog(log: String): (String,String,String)={
                        val visitor = "^([^\\s]+\\s)".r.findFirstIn(log).getOrElse(null) // Matching the visitors from the logs based on pattern
                        val day = "(\\d\\d/\\w{3}/\\d{4})".r.findFirstIn(log).getOrElse(null) // Matching the days from the logs based on pattern
                        val url = "([^\\s]+)(?=\\s+HTTP)".r.findFirstIn(log).getOrElse(null) // Matching the urls from the logs based on pattern
                        (visitor,day,url) // Returning the matches to the map function
                        }
 
  // Method to calculate the top-n most frequent visitors and urls for each day of the trace
  def calculateTrends(trendLimit: Int) {
    
    // Creating the Spark Session
    val sparkSession = SparkSession.builder
                                    .master("local")
                                    .appName("Wsi test")
                                    .getOrCreate()
                                    
    import sparkSession.implicits._
    
    // Reading the log file into a dataset
    val logsDs = sparkSession.read.textFile("logs.gz").cache()
    
    /* Converting logsDs into a dataframe after matching based on the patterns
     * Passing schema to logsDF    
     * Creating a temporary view called trace
     */
    
    val logsDf = logsDs.map( x => matchLog(x)).toDF("visitor","day","url")
    logsDf.createOrReplaceTempView("trace")
    
    
    // Calculating the top-n most frequent visitors and urls for each day of the trace using Spark SQL
    val trendsDf = sparkSession.sql(s"""select F2.day, F2.visitor, H2.url from
                                       (select F1.*, rank() over (partition by day order by cnt desc) as rank1 from 
                                        (select visitor,day,count(visitor) cnt from trace group by visitor,day) F1) F2 LEFT JOIN 
                                          (select H1.*, rank() over (partition by day order by cnt desc) as rank2 from 
                                           (select url,day,count(url) cnt from trace group by url,day) H1) H2
                                              on F2.day = H2.day and F2.rank1 = H2.rank2
                                               where F2.day is not null and F2.rank1 <= ${trendLimit} order by F2.day,F2.rank1""")
    // Displaying the results                                           
    trendsDf.show(false)
    

  }
 
  

}