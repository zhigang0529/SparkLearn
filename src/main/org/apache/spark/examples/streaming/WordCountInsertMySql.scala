package org.apache.spark.examples.streaming

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class WordCountInsertMySql {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("WordCountInsertMySql")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		val lines = ssc.socketTextStream("localhost", 9999)
		val words = lines.flatMap(line => line.split(" "))
		val dstream = words.map((_, 1)).reduceByKey(_ + _)
		//		count.print()

		//		dstream.foreachRDD(rdd => {
		//			val conn = createConnection()
		//			rdd.foreach { record =>
		//				val sql = "insert into WORD_COUNT(word, count) values ('" + record._1 + "'," + record._2 ")"
		//				conn.createStatement().execute(sql)
		//			}
		//		})

		dstream.foreachRDD(rdd => {
			rdd.foreachPartition { partitionOfRecords =>
				if (partitionOfRecords.size > 0) {
					val conn = createConnection()
					partitionOfRecords.foreach(record => {
						val sql = "insert into WORD_COUNT(word, count) values ('" + record._1 + "'," + record._2 + ")"
						conn.createStatement().execute(sql)
					})
					conn.close()
				}
			}
		})

		ssc.start()
		ssc.awaitTermination()
	}

	/**
	  * MySQL连接
	  */
	def createConnection(): Connection = {
		Class.forName("com.mysql.jdbc.Driver")
		val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "123")
		return conn
	}
}
