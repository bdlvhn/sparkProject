package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

import org.apache.spark.sql.SparkSession

object QuesActive {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("QuesActive")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
				
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")
                 }.map{ line => {
                   val xml = XML.loadString(line)
                   (xml.attribute("CreationDate").get,xml.attribute("LastActivityDate").get,line)
                   }
                 }.map {data => {
                   val crDate = format.parse(data._1.toString)
                   val crTime = crDate.getTime
                   
                   val edDate = format.parse(data._2.toString)
                   val edTime = edDate.getTime
                   
                   val timeDiff : Long = edTime - crTime
                   (crDate, edDate, timeDiff, data._3)
                   }
                 }.filter {line => {
                   line._3 / (1000 * 60 * 60 * 24) > 180
                   }
                 }
                 
		result.foreach(println)
		print("Result Count :" + result.count())
//    spark.sparkContext.parallelize(Seq(result.collect),1).saveAsTextFile("/home/hoon/project/spark/4.StackOverflowAnalysis/output/ques-active")

    
    spark.stop
  }
}