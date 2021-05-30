val orders = spark.read.parquet("/user/spark/dataset/retail_db/orders_parquet")
orders.show(5) // parquet 파일 불러오기

val filteredDF = orders.filter($"order_status" === "PENDING_PAYMENT")
filteredDF.show(3) // DF 필터링한 후 확인

val result = filteredDF.select("order_id","order_status")
result.show(3) // 일부 열 선택

result.write.parquet("/user/spark/dataset/output/order-panding-payment") // parquet 파일 쓰기
/* parquet은 자동으로 snappy로 압축되어 저장함 */

result.write.
       option("compression","gzip"). // gzip 형태로 저장
       parquet("/user/spark/dataset/output/orders-gzip")
