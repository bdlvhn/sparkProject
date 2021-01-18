package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

class stb_KPI_5 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
      }
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_5")
				.getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
			
		val record = data.filter { x => x.contains("^0^")}.map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
		  xmlValue
		  }}.filter(x => ((x\\"nv").take(2).takeRight(1)\@"n").equals("BadBlocks"))
    
		val badBlocks = record.map { line => {
		  val valueSeq = line \\ "nv"
		  val badBlocks = (valueSeq.theSeq(0) \@"v").toInt
		  ("BadBlocks",badBlocks)
		  }
		}
  
		badBlocks.saveAsTextFile(args(1))
		
    spark.stop
		  
  }
}