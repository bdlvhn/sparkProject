package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object stb_KPI_4 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
      }
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_4")
				.getOrCreate()
    
		val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
			
		val record = data.filter { x => x.contains("^118^")}.map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
      xmlValue
		  }}	
				
		val Duration = record.map { line => {
		  val valueSeq = line \\ "nv"
		  val durSec = (valueSeq.theSeq(valueSeq.length-1) \@ "v").toInt
		  durSec
		  }
		}
    
		val maxMin = (Duration.max,Duration.min)
		
		spark.sparkContext.parallelize(Seq(maxMin)).saveAsTextFile(args(1))
		
    spark.stop
  }
}