var orderDF = spark.read.parquet("/user/spark/dataset/retail_db/orders_parquet")
orderDF.show(5) // parquet 형태 order 파일 불러오기

var orders = orderDF.select("order_date")
orders.show(5) // date 불러오기

orders = orders.withColumn("order_ts",from_unixtime($"order_date"/1000))
orders.show(5) // unixtime에서 timestamp형으로 변경

orders = orders.withColumn("order_dt",to_date(from_unixtime($"order_date"/1000)))
orders.show(5) // unixtime에서 date형으로 변경
orders.printSchema // 변경한 열 확인

orders = orders.withColumn("year",year($"order_ts")).
                withColumn("month",month($"order_ts")).
                withColumn("day",dayofmonth($"order_ts"))
orders.show(5) // 날짜에 맞는 열 추가, 확인

orders = orders.withColumn("hour",hour($"order_ts")).
                withColumn("min",minute($"order_ts")).
                withColumn("sec",second($"order_ts"))

orders.show(5) // 시간에 맞는 열 추가, 확인

orders = orders.withColumn("curr_dt",current_date()).
                withColumn("curr_ds",current_timestamp())
orders.show(5) // 현재 날짜 및 시간 추가, 확인
