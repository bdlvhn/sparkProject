package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

class stb_KPI_6 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
      }
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_6")
				.getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
			
		val record = data.filter { x => x.contains("^107^")}.map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
		  val deviceId = tokens(5)
		  (deviceId,xmlValue)
		  }}.filter(x => ((x._2\\"nv").take(1)\@"n").equals("ButtonName"))
    
		val devButton = record.map { line => {
		  val valueSeq = line._2 \\ "nv"
		  val buttonName = (valueSeq.take(1)\@"v")
		  (line._1,buttonName)
		  }}.groupByKey()
  
		devButton.saveAsTextFile(args(1))
		
    spark.stop
		  
  }
}