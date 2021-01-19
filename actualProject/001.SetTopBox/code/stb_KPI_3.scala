package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object stb_KPI_3 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
      }
    
		val spark = SparkSession
				.builder
				.appName("stb_KPI_3")
				.getOrCreate()
			
		val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
    
    val record = data.filter { x => x.contains("^102^") || x.contains("^113^") }.map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
      xmlValue
		  }}
    
    val idPrice = record.map { line => {
		    val valueSeq = line \\ "nv"
		    val offerId = (valueSeq.theSeq(valueSeq.length-2) \@ "v")
		    var price = 0d
		    if (!(valueSeq.theSeq(valueSeq.length-1) \@ "v").isEmpty()){
		      price = (valueSeq.theSeq(valueSeq.length-1) \@ "v").toDouble
		    }
		    (offerId, price)
		    }
		  }.groupByKey().sortBy(x => x._2,false)
    
	  idPrice.saveAsTextFile(args(1))
		
    spark.stop
    
  }
}