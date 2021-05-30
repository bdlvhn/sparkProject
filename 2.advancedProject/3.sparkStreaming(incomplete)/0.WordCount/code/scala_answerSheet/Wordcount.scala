package com.df.spark.stream.wc

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Wordcount {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage: WordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("WordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
//                                            10 second batch size

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
//Create a socket stream on target ip:port
//nc -lk 9099

//bin/spark-submit --class com.df.spark.stream.wc.Wordcount ../streamJob.jar localhost 9099