package com.donghoon.wc;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("JavaWordCount").getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//																INPUT	OUTPUT/RETURN-TYPE
      @Override
      public Iterator<String> call(String s) {
        return Arrays.asList(s.split(" ")).iterator();
		/*
		 * Hello there I got a message from you 
		 * 
		 * Hello 
		 * there
		 * I 
		 * got
		 * a
		 * message
		 * from
		 * you
		 */
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//																		 INPUT	  OUTPUT/RETURN
    	@Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<>(s, 1);
          
  		/* 
		 * Hello, 1
		 * there, 1
		 * I, 1
		 * got, 1
		 * a, 1
		 * message, 1
		 * from, 1
		 * you, 1
  		 */
        }
      });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
//    	  			INPUT			  OUTPUT/RETURN
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
//          	  1   1
//          	  2   1
//          	  3   1
          /*
           * got		[1 1 1 1 1 1 1 1 1.......]
           * message	[1 1 1 1 1 1 1 1 1.......]
           * ....
           */
        }
      });

	counts.saveAsTextFile(args[1]);
    spark.stop();
  }
}
//bin/spark-submit --class com.donghoon.wc.JavaWordCount ../sparkJob.jar ../about-dataflair.txt wc-out-01