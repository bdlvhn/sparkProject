// spark-shell --packages org.apache.spark:spark-avro_2.12:2.4.5

val prodDF = spark.read.format("avro").
             load("/user/spark/dataset/retail_db/products_avro")
prodDF.show(5) // avro 파일 읽고 확인

val filteredData = prodDF.filter($"product_price" > 1000.0)
filteredData.show(5) // product_price로 필터링한 데이터 확인

filteredData.write.
             format("avro").
             option("compression","snappy").
             save("/user/spark/dataset/output/products-avro-snappy") // 데이터 avro 형태로 저장
