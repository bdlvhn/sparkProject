package com.hoon.ahc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ahc_KPI_4 {
  def main(args:Array[String]){
   
    if (args.length < 1) {
			System.err.println("Aadhar Card Analysis <Input-File> is missing");
			System.exit(1);
      }
    
    val spark = SparkSession
				.builder
				.appName("aadharCard_KPI_4")
				.master("local")
				.getOrCreate()
    
	 val date = StructField("date", DataTypes.IntegerType)
   val registrar = StructField("registrar", DataTypes.StringType)
   val private_agency = StructField("private_agency", DataTypes.StringType)
   val state = StructField("state", DataTypes.StringType)
   val district = StructField("district", DataTypes.StringType)
   val sub_district = StructField("sub_district", DataTypes.StringType)
   val pin_code = StructField("pin_code", DataTypes.StringType)
   val gender = StructField("gender", DataTypes.StringType)
   val age = StructField("age", DataTypes.IntegerType)
   val generated = StructField("generated", DataTypes.IntegerType)
   val rejected = StructField("rejected", DataTypes.IntegerType)
   val mobile_no = StructField("mobile_no", DataTypes.IntegerType)
   val email_id = StructField("email_id", DataTypes.IntegerType)
     
   val fields = Array(date, registrar, private_agency, state, district, sub_district, pin_code, gender, age, generated, rejected, mobile_no, email_id)
     
   val schema = StructType(fields)
   
   import spark.implicits._
   
   val data = spark.read.
              schema(schema).
              csv("/home/hoon/project/sparkActual/002.Aadhar_Card/data/aadhar_data.csv").as[AadharData]
//   val data = spark.read.schema(schema).csv(args(0)).as[AadhaarData]
   
   val mobileUDF = udf {
    (mobile_no: Integer) => if(mobile_no>0) {"Y"} else {"N"}
    } 
    
   data.withColumn("mobile_yn",mobileUDF($"mobile_no")). // kpi_4_01
   cube("age","mobile_yn").
   agg((sum("generated")).as("genSum")).
   orderBy("age","mobile_yn").
   cube("age","mobile_yn").count().// when mobile = y then sum, when mobile = n then sum
   show()
   
   data.select(countDistinct("pin_code")).show() // kpi_4_02
   data.select("pin_code").distinct.count()

   data.filter($"state" isin ("Uttar Pradesh","Maharashtra")). // kpi_4_03
   groupBy("state").
   agg(sum("rejected").as("rejSum")).show()
    
  }
}