package com.hoon.stb

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.xml.XML

object stb_KPI_1 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
			System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing");
			System.exit(1);
		}
    
    val spark = SparkSession
				.builder
				.appName("stb_KPI_1")
				.getOrCreate()
				
		val data = spark.read.textFile(args(0)).rdd
//    val data = spark.read.textFile("/home/hoon/project/spark/001.SetTopBox/data/Set_Top_Box_Data.txt").rdd
    
		val record = data.filter { x => x.contains("^100^") }.map { line => {
		  val tokens = line.split("\\^")
//		  val serverUniqueId = tokens(0).toInt
//		  val requestType = tokens(1).toInt
		  val eventId = tokens(2).toInt
//		  val timeStamp = tokens(3)
		  val xmlValue = XML.loadString(tokens(4))
		  val deviceId = tokens(5)
//		  val secondaryTimeStamp = tokens(6)
		  (eventId,(deviceId,xmlValue))
		  }}
		
		val maxDurTop5 = record.map { line => {
		  val valueSeq = line._2._2 \\ "nv"
		  val duration = (valueSeq.theSeq(valueSeq.length-5) \@ "v").toInt
  		 val channelNum = (valueSeq.theSeq(valueSeq.length-6) \\"nv" \@ "v").toInt
		  (duration, (line._2._1, channelNum))
		  }}.sortByKey(false).take(10)
		
		val chNumSum = record.map { line => {
		  val valueSeq = line._2._2 \\ "nv"
		  val chType = (valueSeq.theSeq(valueSeq.length-2) \@ "v")
		  chType
		  }}.filter(x => x.contains("LiveTVMediaChannel")).count()
		
		val TotLiveCh = "Total number Of LiveTVMediaChannel : " + chNumSum
		
		Seq(maxDurTop5,TotLiveCh).saveAsTextFile(args(1)) 
		
		spark.stop
		
    }

  }
}