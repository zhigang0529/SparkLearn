package org.apache.spark.examples
import org.apache.spark.sql.SparkSession

object SimpleApp {
	def main(args: Array[String]) {
		val logFile =  "D:\\Idea\\SparkLearn\\out\\Install-Windows-zip.txt"
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		val logData = spark.read.textFile(logFile).cache()
		val numAs = logData.filter(line => line.contains("home")).count()
		val numBs = logData.filter(line => line.contains("and")).count()
		println(s"Lines with home: $numAs, Lines with and: $numBs")
		spark.stop()
	}
}
