package com.hoon.wc

import scala.math.random

import org.apache.spark.sql.SparkSession

object Wordcount {
	def main(args: Array[String]) {
	  
		if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
      System.exit(1);
    }
		
		val spark = SparkSession
				.builder
				.appName("Wordcount")
				.getOrCreate()

    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.flatMap { line => {
      line.split(" ")
      } }
    .map { words => (words, 1)
    }
    .reduceByKey(_+_)
    
    result.saveAsTextFile(args(1))
    
    spark.stop
	}
}