package com.df.spark.flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.flume._
import com.df.spark.flume._

object WeblogParser {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: Weblogs Parser <hostname> <port>")
			System.exit(1)
		}
		

		val sparkConf = new SparkConf().setAppName("Weblogs Parser")
				val ssc = new StreamingContext(sparkConf, Seconds(1))

				val rawLines = FlumeUtils.createStream(ssc, args(0), args(1).toInt)

				val lines = rawLines.map{record => {
					(new String(record.event.getBody().array()))
				}
		    }
		
				val result = lines.map{data => {
				  LogParser.parse(data)
				}  
				}

				result.print()
				ssc.start()
				ssc.awaitTermination()
	}
}
//bin/spark-submit --jars ../spark-streaming-flume-assembly_2.11-2.0.0.jar  --class com.df.spark.flume.WeblogParser ../streamFlumeJob.jar localhost 9999