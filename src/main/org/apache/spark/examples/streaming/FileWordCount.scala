package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
	val path = "/opt/spark-2.4.0/out"

	def main(args: Array[String]) {

		val sparkConf = new SparkConf().setAppName("FileWordCount")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		// Create the FileInputDStream on the directory and use the
		// stream to count words in new files created
		val lines1 = ssc.textFileStream(path)
		val words1 = lines1.flatMap(line => line.split(" "))
		val count1 = words1.map((_, 1)).reduceByKey(_ + _)
		count1.print()
		ssc.start()
		ssc.awaitTermination()
	}

	/**
	spark-submit \
	--class org.apache.spark.examples.streaming.FileWordCount \
	--master spark://centos:7077  \
	--executor-memory 4G \
	--total-executor-cores 2 \
	/opt/spark-2.4.0/lib/spark-learn-1.0.jar
	  */
}
