package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

import org.apache.spark.sql.SparkSession

object TagQuesScore {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("TagQuesScore")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
    val format3 = new SimpleDateFormat("yyyy-MM-dd")
    
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")
                 }.map {line => {
                   val xml = XML.loadString(line)
                   val tags = xml.attribute("Tags").get.toString.replaceAll("&lt;"," ").replaceAll("&gt;"," ").trim()
                   val score = xml.attribute("Score").get.toString
                   (tags, score, line)
                     }
                 }.filter {data => {
                   data._1.contains("hadoop") || data._1.contains("spark")
                   }
                 }.map {data => {
                   (data._1, data._2)
                   }
                 }.sortBy(x=>x._2,false).take(10)
                 
    result.foreach(println)
    
    spark.stop
  }
}