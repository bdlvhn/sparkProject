package com.hoon.wba

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.lang.Long

object MostPopulous {
  def main(args: Array[String]){
    
    if (args.length < 2) {
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
    
    val spark = SparkSession
                .builder
                .appName("Most populous countries")
//                .master("local")
                .getOrCreate()
                
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.map { line => {
      val pop = line.getString(9).replaceAll(",","") // pop : total-population
      var pop_num = 0L
      if (pop.length > 0)
        {pop_num = Long.parseLong(pop)}
      (line.getString(0),pop_num)
      }
    }.groupByKey()
    .map(rec => (rec._1,rec._2.max))
    .sortBy(rec => rec._2, false)
    .take(10)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
  }
}