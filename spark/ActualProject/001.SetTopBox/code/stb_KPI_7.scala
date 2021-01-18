package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

class stb_KPI_7 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File-> is missing");
			System.exit(1);
      }
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_7")
				.getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
			
		val record = data.filter { x => x.contains("^115^") | x.contains("^118^")}.
		filter { x => x.split("\\^")(4).substring(0,3).equals("<d>") && x.split("\\^")(4).substring(x.split("\\^")(4).length-4,x.split("\\^")(4).length).equals("</d>")}.
		map { line => {
		  val tokens = line.split("\\^")
		  val xmlValue = XML.loadString(tokens(4))
		  xmlValue
		  }}
    
		val progDuration = record.filter(x => ((x\\"nv").theSeq(1)\@"n").equals("ProgramId") &&
		      ((x\\"nv").theSeq((x\\"nv").length-1)\@"n").equals("DurationSecs")).
		      map { line => {
		  val valueSeq = line \\ "nv"
		  val programId = valueSeq.theSeq(1)\@"v"
		  val durSecs = valueSeq.theSeq(valueSeq.length-1)\@"v"
		  (programId,durSecs)
		  }}.groupByKey()
		  
		val freqOnce = record.filter(x => ((x\\"nv").takeRight(2).take(1)\@"n").equals("Frequency")).
		map { line => {
		  val valueSeq = line \\ "nv"
		  val freq = valueSeq.takeRight(2).take(1)\@"v"
		  freq
		}}.filter(x => x.equals("Once")).count()
  
		spark.sparkContext.parallelize(Seq(progDuration,freqOnce)).saveAsTextFile(args(1))
		
    spark.stop
		  
  }
}