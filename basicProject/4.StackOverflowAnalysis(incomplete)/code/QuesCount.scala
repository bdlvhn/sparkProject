package com.hoon.stackoverflow

import org.apache.spark.sql.SparkSession

object QuesCount {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir","/home/hoon/hadoop-2.7.7")
		System.setProperty("spark.sql.warehouse.dir","/home/hoon/spark-2.4.7-bin-hadoop2.7/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("QuesCount")
				.master("local")
				.getOrCreate()
				
		val data = spark.read.textFile("/home/hoon/project/spark/4.StackOverflowAnalysis/input/posts.xml").rdd
		
		val result = data.filter {line => line.trim().startsWith("<row")
                 }.filter {line => line.contains("PostTypeId=\"1\"")}
    
    result.foreach(println)
    println("Total Count: " + result.count())
    
    spark.stop

  }
}