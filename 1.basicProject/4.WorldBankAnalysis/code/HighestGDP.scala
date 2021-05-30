package com.hoon.wba

import org.apache.spark.sql.SparkSession
import java.lang.Double
import java.lang.Long

object HighestGDP {
  def main(args: Array[String]){
    
    if (args.length<2){
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
    
    val spark = SparkSession
               .builder
               .appName("Highest GDP growth")
               .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.map {line =>  {
      if (line.isNullAt(18)) {
	    (line.getString(0),0L)
	    }
      else {
      val GDP = Long.parseLong(line.getString(18).replaceAll(",",""))
      (line.getString(0),GDP)
        }
      }
    }.groupByKey()
    .map{rec => {
      val values = rec._2.takeRight(2).toList
      val gdp_09 = values(0)
      val gdp_10 = values(1)
      (rec._1, gdp_10 - gdp_09)
      }
    }.sortBy(_._2,false)
    .take(10)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
  }
}