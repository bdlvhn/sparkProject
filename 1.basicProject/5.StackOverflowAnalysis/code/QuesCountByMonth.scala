package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object QuesCountByMonth {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("QuesCountByMonth")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
				
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")
                 }.flatMap{ line => {
                   val xml = XML.loadString(line)
                   xml.attribute("CreationDate")
                   }
                 }.map { line => {
                 (format2.format(format.parse(line.toString())).toString(),1)
                   }
                 }.reduceByKey(_+_)
                 .sortByKey()

    result.foreach(println)
    
    spark.stop
  }
}