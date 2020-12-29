package com.donghoon.ns;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaNumberSum {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaNumberSum <Input-File> <Output-File>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("JavaNumberSum").getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> numbers = lines.flatMap(new FlatMapFunction<String, String>() {
//																INPUT	OUTPUT/RETURN-TYPE
      @Override
      public Iterator<String> call(String s) {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> numArray = numbers.mapToPair(new PairFunction<String, String, Integer>() {
//																		 INPUT	  OUTPUT/RETURN
    	@Override
        public Tuple2<String, Integer> call(String s) {
    	  int num = Integer.parseInt(s);
          
    	  return new Tuple2<>("numberSum",num);
         
        }
      });

    JavaPairRDD<String, Integer> sum = numArray.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
//    	  			INPUT			  OUTPUT/RETURN
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

	sum.saveAsTextFile(args[1]);
    spark.stop();
  }
}