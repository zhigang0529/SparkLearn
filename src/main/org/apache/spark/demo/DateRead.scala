package org.apache.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object DateRead {
	val txtFile = "D:\\Idea\\SparkLearn\\out\\test.txt"
	private var conf: SparkConf = null
	private var sc: SparkContext = null

	def initSC(): Unit = {
		conf = new SparkConf().setAppName("DateRead").setMaster("local[2]")
		sc = new SparkContext(conf)
	}

	def reduceByKey(): Unit = {
//		val lines = sc.textFile(txtFile)
//		val pairs = lines.flatMap(line => line.split(" ")).map(word => (word, 1))
//		val reduce = pairs.reduceByKey((x, y) => (x + y)).foreach(println)
	}

	def leftOuterJoin(): Unit = {
//		val a1 = List((2, (200, 300)), (3, (400, 500)), (4, (500, 600)))
//		val a2 = List((2, (200, 300)), (3, (400, 500)), (1, (500, 600)))
//		val rdd1 = sc.parallelize(a1)
//		val rdd2 = sc.parallelize(a2)
//		//		rdd1.leftOuterJoin(rdd2).foreach(println)
//		val rdd3 = rdd1.leftOuterJoin(rdd2)
//		rdd3.map { case (id, (x, y)) =>
//			val (y3, y2) = y.getOrElse((null, null))
//			(id, x._1, x._2, y3, y2)
//		}.foreach(println)

	}

//	def groupByKey(): Unit = {
//		val arr = new Array[(Int, Int)](10)
//		arr(0) = (1, 2); arr(1) = (3, 3);	arr(2) = (2, 1);		arr(3) = (3, 1);		arr(4) = (1, 3)
//		arr.filter(_ != null).foreach(println)
//		val ele = arr.filter(_ != null).map { case (n1, n2) => (n1, n2) }
//		val rddA = sc.parallelize(ele)
//		rddA.groupByKey().foreach(println)
//	}

	/**
	  * 当被调用的两个 DStream 分别含有 (K, V) 和 (K, W) 键值对时 , 返回一个 (K, Seq[V], Seq[W]) 类型的新的 DStream
	  */
//	def cogroup(): Unit = {
//		val rdd1 = sc.parallelize(List((1, 2), (1, 3), (1, 4), (2, 10), (2, 10), (3, 11), (3, 12), (4, 100)))
//		val rdd2 = sc.parallelize(List((1, 2), (1, 3), (10, 911)))
//		rdd1.cogroup(rdd2).map { case (id, (f1, f2)) =>
//			val f = if (f1.isEmpty) -1 else f1
//			(id, f1, f2)
//		}.foreach(println)
//	}

	def option(): Unit = {
		val myMap1: Map[String, Boolean] = Map("key1" -> true, "key3" -> false)
		val myMap2 = myMap1 + ("key2" -> false)
		val x = myMap2.get("key1")
		println(x)
	}

//	def getOption(a: Boolean, b: Boolean): Option[Boolean] = {
//		if (a == null && b == null) None
//		else if (a != null && b == null) Some(a)
//		else if (a == null && b != null) Some(b)
//		else if (a != null && b != null) Some(a || b)
//		else None
//	}

	def array(): Unit = {
		val arr1: Array[(String, Int)]= Array(("x", 1), ("y", 2), ("z", 3))
		val arr2: Array[(String, Int)]= Array(("x", 99), ("m", 110), ("y", 110))
		val arr3 = arr1 ++ arr2
//		arr3.foreach(println)
		val rdd = sc.parallelize(arr3, 1)
		rdd.map{case (x, y) => (x, y)
		}.foreach(println)
		println("reduce by key")
		rdd.reduceByKey((x, y) => (x + y)).foreach(println)
		println("group by key")
		rdd.groupByKey().foreach(println)
	}

	def flatMap(): Unit = {
		val a = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9))
		val rddTest = sc.parallelize(a)
		a.flatMap { case x => x.map(x => x + 100) }.foreach(println)
		a.flatMap { case x => x.map(x => x * 30) }.foreach(println)

	}

	def sortBy(): Unit = {
		val list = List((11, 23, 3), (14, 35, 6), (-2, 2, 3))
		val rdd = sc.parallelize(list)
		val result = rdd.sortBy(r => (r._1, r._2, r._3), ascending = false).collect()
//		result.foreach(println)
		for(e <- result)
			println(e._1)
	}

	def rdds(): Unit = {
		val rdd1 = sc.parallelize(Array("apple", "peach", "banana", "red", "apple"))
		val rdd2 = sc.parallelize(Array("green", "black", "green", "red"))
		rdd1.distinct()
		val rdd3 = rdd1.union(rdd2)
		rdd3.foreach(println)
		val rdd4 = rdd1.intersection(rdd2)
		rdd4.foreach(println)
	}

	def reduce(): Unit ={
		val rdd1 = sc.parallelize(Array(1,2,3,4,5,6))
		val total = rdd1.reduce((x, y) => x + y)
		println("total:" + total)
		println("take 3 : " + rdd1.take(3))
	}

	def main(args: Array[String]): Unit = {
		initSC()
		rdds()
	}
}
