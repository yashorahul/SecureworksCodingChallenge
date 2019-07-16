# SecureworksCodingChallenge
Coding challenge to determine most frequent visitors and urls for each day of the trace

This is a maven project created using eclipse.

This project has 2 parts

1) LogFileDownloader.scala --> To download the log file into our workspace or any desired location
2) TrendCalculator.scala --> To read the downloaded log file and determine most frequent visitors and urls for each day of the trace

Steps to run the project:

1) Import the maven project in eclipse 
2) Right click on LogFileDownloader.scala --> Run As --> Run Configurations
3) Pass the desired trends limit as an argument and Run

Note: I couldn't package the application in a docker container as I was not able to install Docker on my work laptop due to permission issues.
