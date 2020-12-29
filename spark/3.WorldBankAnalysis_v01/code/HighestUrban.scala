package com.hoon.wba

import org.apache.spark.sql.SparkSession
import java.lang.Double
import java.lang.Long


object HighestUrban {
	def main(args: Array[String]) = {
	  
    if (args.length < 2) {
      System.err.println("Usage: WorldBankAnalysis <in> <out>")
      System.exit(1)
    }
			
	  val spark = SparkSession
				.builder
				.appName("Highest urban population")
//				.master("local")
				.getOrCreate()
				
		val data = spark.read.csv(args(0)).rdd
		
	  val result = data.map { line => {
	    if (line(10)==null) {
	    (line.getString(0),0L)
	    }
	    else {
	    val pop = line.getString(10).replaceAll(",","") // pop : urban-population
      var pop_num = 0L
      if (pop.length > 0)
        {pop_num = Long.parseLong(pop)}
      (line.getString(0),pop_num)
	    }
    }}
		.sortBy(rec => rec._2,false)
		.first

		spark.sparkContext.parallelize(Seq(result)).saveAsTextFile(args(1))
		
		spark.stop
	}
}