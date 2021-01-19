package com.hoon.wba

import org.apache.spark.sql.SparkSession
import java.lang.Double
import java.lang.Long


object HighestGrowth {
  
  def main(args: Array[String]){
    
    if (args.length < 2) {
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
    
    val spark = SparkSession
                .builder
                .appName("Highest population growth")
//                .master("local")
                .getOrCreate()
                
    val data = spark.read.csv("/home/hoon/project/spark/3.WorldBankAnalysis/input/world_bank_indicators.csv").rdd
    
    val result = data.map{ line => {
      val pop = line.getString(9).replaceAll(",","") // pop : total-population
      var pop_num = 0D
      if (pop.length > 0)
        pop_num = Double.parseDouble(pop)
      (line.getString(0),pop_num)
      }}.groupByKey
    .map{ rec => {
		  val minPop = (rec._2.min)
		  val maxPop = (rec._2.max)
		  var perGrowth = 0D
      perGrowth = ((maxPop-minPop)/minPop)*100
      (rec._1, perGrowth)
    }}
    .sortBy(rec => rec._2, false)
    .take(10)

    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
  }
}