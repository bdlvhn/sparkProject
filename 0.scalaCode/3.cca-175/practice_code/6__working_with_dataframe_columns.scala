var custDF = spark.read.option("header",true).csv("/user/spark/dataset/retail_db/customers")
custDF.show(5) // csv 파일 불러오기

custDF = custDF.drop("customer_email","customer_password","customer_street") 
custDF.show(5) // 열 3개 빼기

custDF = custDF.withColumn("Country",lit("USA"))
custDF.show(5) // 원하는 값을 입력한 새로운 열 추가

custDF = custDF.withColumn("customer_name",concat_ws(" ",$"customer_fname",$"customer_lname")) // ws : with separate
custDF.show(5) // 기존 열을 결합한 새로운 열 추가

custDF = custDF.drop("customer_fname","customer_lname")
custDF.show(5) // 이름 열 빼기

custDF.printSchema
custDF = custDF.withColumn("customer_zipcode",col("customer_zipcode").cast("Integer"))
// 열 데이터형 변환

custDF = custDF.withColumn("Country",col("Country").substr(0,2))
custDF.show(5) // 열 데이터 변환

custDF = custDF.withColumnRenamed("Country","customer_country")
custDF.show(5) // 열 이름 재정의

