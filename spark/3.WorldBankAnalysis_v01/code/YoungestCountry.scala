package com.hoon.wba

import org.apache.spark.sql.SparkSession
import java.lang.Double
import java.lang.Long

object YoungestCountry {
  def main(args: Array[String]){
    
    if (args.length<2){
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
    
    val spark = SparkSession
                .builder
                .appName("Youngest Population")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
        val result = data.map {line => {
      var pop = 0L // pop : ages 15-64
      if (!line.isNullAt(16)){
        pop = line.getString(16).toLong
      }
      (line.getString(0),pop)
      }}.groupByKey
    .map {rec => {
      val pop_first = rec._2.toList(0)
      val pop_last = rec._2.toList(10)
      (rec._1,pop_last-pop_first)
    }}.sortBy(_._2,false)
    .take(10)
                
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop            
  }
}