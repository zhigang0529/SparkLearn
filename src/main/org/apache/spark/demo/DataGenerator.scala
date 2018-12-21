package org.apache.spark.demo
import java.io.FileWriter
import java.io.File
import scala.util.Random
object DataGenerator {
	def main(args: Array[String]): Unit ={
		val writer= new FileWriter(new File("D:\\Idea\\SparkLearn\\out\\simple_date.txt"), false)
		val random = new Random()

		for(i <- 1 to 1000){
			writer.write(i + " " + random.nextInt(100))
			writer.write(System.getProperty("line.separator"))
		}
		writer.flush()
		writer.close()
	}
}
