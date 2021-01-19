package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

import org.apache.spark.sql.SparkSession

object QuesDateTag {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("QuesDateTag")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
    val format3 = new SimpleDateFormat("yyyy-MM-dd")
				
    val startTime = format3.parse("2015-01-01").getTime
    val endTime = format3.parse("2015-12-31").getTime
    
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")
                 }.map{ line => {
                   val xml = XML.loadString(line)
                   val crDate = xml.attribute("CreationDate").get.toString
                   val tags = xml.attribute("Tags").get.toString
                   (crDate,tags,line)
                   }
                 }.filter {data => {
                   var flag = false
                   val crTime = format.parse(data._1).getTime
                   if (crTime > startTime && crTime < endTime &&
                       (data._2.toLowerCase.contains("hadoop") ||
                       data._2.toLowerCase.contains("spark") ||
                       data._2.toLowerCase.contains("bigdata")))
                       flag = true
                   flag
                   }
                 }
    
    result.foreach(println)
    print("Result Count :" + result.count())
    
    spark.stop
  }
}