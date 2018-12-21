package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  *
  * Usage: NetworkWordCount <hostname> <port>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
  */
object NetworkWordCount {
	def main(args: Array[String]) {
		val outPath = "/opt/spark-2.4.0/out/networkwordcount"
//		if (args.length < 2) {
//			System.err.println("Usage: NetworkWordCount <hostname> <port>")
//			System.exit(1)
//		}
		val args: Array[String] = Array("localhost", "9999")

		// Create the context with a 1 second batch size
		val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		// Create a socket stream on target ip:port and count the
		// words in input stream of \n delimited text (eg. generated by 'nc')
		// Note that no duplication in storage level only for running locally.
		// Replication necessary in distributed scenario for fault tolerance.
		val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
		wordCounts.print()
//		if(wordCounts != null)
//			wordCounts.saveAsTextFiles(outPath)
		ssc.start()
		ssc.awaitTermination()
	}
}
