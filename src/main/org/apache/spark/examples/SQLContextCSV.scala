package org.apache.spark.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContextCSV {

	case class Person(sutype: Int, phone: String)

	val path = "D:\\Idea\\SparkLearn\\out\\person.csv"

	def main(args: Array[String]) {
		val conf = new SparkConf().setMaster("local[2]").setAppName("SQLContextCSV")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		import sqlContext.implicits._
		val data = sc.textFile(path).map(_.split(",")).map(p => Person(p(0).toInt, p(2))).toDF()
		data.registerTempTable("PERSON")

		val q = sqlContext.sql("select count(*), phone from PERSON where sutype='2' group by phone")
		var count = sqlContext.sql("select count(*) from PERSON");
		println("----------------->")
		println(q)
		q.show()

		println("all count==========" + q.count())
	}
}
