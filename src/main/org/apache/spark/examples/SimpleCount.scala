package org.apache.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleCount {
	val txtFile = "D:\\Idea\\SparkLearn\\out\\test.txt"
//	val txtFile = "/opt/spark-2.4.0/conf/spark-env.sh"

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("TrySparkStreaming").setMaster("local[2]")
		// Create spark context
		val sc = new SparkContext(conf)
		val txtData = sc.textFile(txtFile)
		txtData.cache()
		txtData.count()

		val wcData =
			txtData.flatMap { line => line.split(" ") }.map { word =>
				(word, 1)
			}.reduceByKey(_ + _)
		wcData.collect().foreach(println)
		sc.stop
	}
}
