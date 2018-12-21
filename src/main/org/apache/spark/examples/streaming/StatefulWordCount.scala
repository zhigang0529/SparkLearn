package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey
  * Return a new "state" DStream where the state for each key is updated by
  * applying the given function on the previous state of the key and the new values for the key.
  * This can be used to maintain arbitrary state data for each key.
  */
object StatefulWordCount {

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(5))

		//checkpoint目录
		ssc.checkpoint("./out")

		val text = ssc.socketTextStream("localhost", 9999)
		val line = text.flatMap(_.split(" "))
		val result = line.map((_, 1))
		val stateful = result.updateStateByKey[Int](updateFunc _)

		stateful.print()
		ssc.start()
		ssc.awaitTermination()
	}

	/**
	  * 当前数据更新到已有数据
	  */
	def updateFunc(currValue: Seq[Int], lastValue: Option[Int]): Option[Int] = {
		val current = currValue.sum
		val last = lastValue.getOrElse(0)
		Some(current + last)
	}

	/**
	spark-submit   --class org.apache.spark.examples.streaming.StatefulWordCount   --master spark://centos:7077    --executor-memory 4G   --total-executor-cores 2   /opt/spark-2.4.0/lib/spark-learn-1.0.jar
	  */
}
