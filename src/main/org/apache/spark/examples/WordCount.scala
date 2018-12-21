package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
	//	System.setProperty("hadoop.home.dir", "D:\\Work\\Hadoop\\hadoop-common-2.2.0-bin-master")
	var txtPath = "D:\\Idea\\SparkLearn\\out\\test.txt"
	var outPath = "D:\\Idea\\SparkLearn\\out\\output"

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val input = sc.textFile(txtPath)
//		val numAs = input.filter(line => line.contains("a")).count()
//		val lines = input.flatMap(line => line.split(" "))
//		val count = lines.map(word => (word, 1))
//		val result = count.reduceByKey { case (x, y) => x + y }
//		result.collect().foreach(println)
		//		result.saveAsTextFile(outPath)

	}
}