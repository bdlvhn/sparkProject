val orders = spark.sql("select * from default.orders")
orders.show(5) // sparksql을 통해 자료 가져옴

orders.write.format("hive").
             saveAsTable("default.orders_replica") // hive에 테이블로 저장

spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
// partitioning 설정

orders.write.format("hive").
             partitionBy("order_date"). // order_date 기준으로 파티셔닝
             saveAsTable("default.orders_partitioned")

spark.sql("""
        create table orders_parquet(
        order_id int,
        order_date string,
        order_customer_id int,
        order_status string
        ) STORED AS PARQUET""")
// parquet 형태로 테이블 생성

orders.write.format("hive").
             mode("overwrite").
             saveAsTable("default.orders_parquet")
// 테이블에 데이터 추가 (overwrite 형태)

orders.write.format("hive").
             mode("append").
             saveAsTable("default.orders_parquet")
// 테이블에 데이터 추가 (append 형태)
