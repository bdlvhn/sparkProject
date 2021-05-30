package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

import org.apache.spark.sql.SparkSession

object AvgAnsTime {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("AvgAnsTime")
				.master("local")
				.getOrCreate()
		
		val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
		val format2 = new SimpleDateFormat("yyyy-MM")
    val format3 = new SimpleDateFormat("yyyy-MM-dd")
    
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val baseData = data.filter {line => line.trim().startsWith("<row")
                 }.map {line => {
                   val xml = XML.loadString(line)
                   var aaId = "";
                   if (xml.attribute("AcceptedAnswerId")!=None)
                   {
                     aaId = xml.attribute("AcceptedAnswerId").get.toString
                   }
                   val crDate = xml.attribute("CreationDate").get.toString
                   val rId = xml.attribute("Id").get.toString
                   (rId, aaId, crDate)
                   }}
    
 		val aaData = baseData.map {data => {
 		  (data._2,data._3)
 		  }}.filter {data => { data._1.length > 0}}
    
    val rData = baseData.map { data => {
      (data._1,data._3)
      }}
    
    val joinData = rData.join(aaData).map{ data => {
      val ansTime = format.parse(data._2._1).getTime
      val quesTime = format.parse(data._2._2).getTime
      val diff : Float = ansTime - quesTime
      val timeDiff : Float = diff / (1000 * 60 * 60) // hour
      timeDiff
      }}
    
    val result = joinData.sum / joinData.count
    
    println(result)
//    spark.sparkContext.parallelize(Seq(result),1).saveAsTextFile("/home/hoon/project/spark/4.StackOverflowAnalysis/output/avg-ans-time")
    spark.stop
  }
}