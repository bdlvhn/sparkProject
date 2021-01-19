//1.Calculate sales breakdown by product category across all of the stores.
package com.donghoon.retail;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class ProductSales {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: Retail Analysis <INP-PATH> <OUTPUT-PATH>");
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder().appName("RetailAnalysis").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		JavaPairRDD<String, Float> pairData = lines.mapToPair(new PairFunction<String, String, Float>() {
			@Override
			public Tuple2<String, Float> call(String s) {
//				"2012-01-01      09:00   San Jose        Men's Clothing  214.05  Amex"
				final String[] dataTokens = s.trim().split("\t");
				return new Tuple2<String, Float>(dataTokens[3], Float.parseFloat(dataTokens[4]));
			}
		});

		JavaPairRDD<String, Float> result = pairData.reduceByKey(new Function2<Float, Float, Float>() {
			@Override
			public Float call(Float i1, Float i2) {
				return i1 + i2;
			}
		});
		
		result.saveAsTextFile(args[1]);
		spark.stop();
	}
}