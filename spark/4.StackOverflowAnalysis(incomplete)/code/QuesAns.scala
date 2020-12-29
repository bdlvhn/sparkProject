package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

import org.apache.spark.sql.SparkSession

object QuesAns {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("QuesAns")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
				
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")
                 }.map{ line => {
                   val xml = XML.loadString(line)
                   (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()),line)
                   }
                 }.filter {x => x._1 > 2}
    
    result.foreach(println)
    print("Result Count :" + result.count())
    
    spark.stop
  }
}