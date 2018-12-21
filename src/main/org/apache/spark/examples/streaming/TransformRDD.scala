package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform
  * Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream.
  * This can be used to do arbitrary RDD operations on the DStream.
  */
object TransformRDD {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("TransformRDD")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		//Black list
		val blacks = List("zs", "ls")
		val blackRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

		val lines = ssc.socketTextStream("localhost", 9999)
		val result = lines.map(line => (line.split(",")(1), line)).transform(rdd => {
			rdd.leftOuterJoin(blackRDD)
				.filter(x => x._2._2.getOrElse(false) != true)
				.map(x => x._2._1)
		})

		result.print()
		ssc.start()
		ssc.awaitTermination()
	}

	/**
	  * 2016,zs
	  * 2017,ls
	  * 2018,ww
	  */
	/**
	  * spark-submit \
	  * --class org.apache.spark.examples.TransformRDD \
	  * --master spark://centos:7077  \
	  * --executor-memory 2G \
	  * --total-executor-cores 2 \
	  * /opt/spark-2.4.0/lib/spark-learn-1.0.jar
	  */

}
