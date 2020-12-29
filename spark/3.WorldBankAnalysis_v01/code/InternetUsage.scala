package com.hoon.wba

import org.apache.spark.sql.SparkSession
import java.lang.Double
import java.lang.Long

object InternetUsage {
  def main(args: Array[String]){
    
    if (args.length<2){
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
    
    val spark = SparkSession
                .builder
                .appName("Internet Usage Growth")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.map {line => {
      var usage = 0L
      if (!line.isNullAt(5)){
        usage = line.getString(5).toLong
      }
      (line.getString(0),usage)
      }}.groupByKey
    .map {rec => {
      val usage_first = rec._2.toList(0)
      val usage_last = rec._2.toList(10)
      (rec._1,usage_last-usage_first)
    }}.sortBy(_._2,false)
    .take(10)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
  }
}