package com.dataflair.wc;

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
		 * DataFlair is the leading Training Provider of Big Data Technologies 
		 * 
		 * DataFlair 
		 * is 
		 * the 
		 * leading 
		 * Training 
		 * Provider 
		 * of 
		 * Big 
		 * Data 
		 * Technologies
		 */
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<>(s, 1);
          
  		/* 
  		 * DataFlair, 1
  		 * is, 1
  		 * the, 1
  		 * leading, 1
  		 * Training, 1
  		 * Provider, 1
  		 * of, 1
  		 * Big, 1
  		 * Data, 1
  		 * Technologies, 1
  		 */
        }
      });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
          /*
           * DataFlair	[1 1 1 1 1 1 1 1 1.......]
           * Training	[1 1 1 1 1 1 1 1 1.......]
           * ....
           */
        }
      });

	counts.saveAsTextFile(args[1]);
    spark.stop();
  }
}
//bin/spark-submit --class com.dataflair.wc.JavaWordCount ../sparkJob.jar ../about-dataflair.txt wc-out-01