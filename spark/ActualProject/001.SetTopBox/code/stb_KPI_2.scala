package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

class stb_KPI_2 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
		}
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_2")
				.getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
    
    val record = data.filter { x => x.contains("^101^") }.map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
      xmlValue
		  }}
    
    val poweredDeviceSum = record.map { line => {
      val valueSeq = line \\ "nv"
      val devState = valueSeq.theSeq(valueSeq.length-3) \@"v"
      (devState,1)
    }}.reduceByKey(_+_)
    
    poweredDeviceSum.saveAsTextFile(args(1))
		
    spark.stop
    
    
  }
}