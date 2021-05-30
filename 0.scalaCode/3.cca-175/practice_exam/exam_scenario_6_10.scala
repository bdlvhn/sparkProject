/* Scenario 06 */
spark.catalog.listTables().show()
val result = spark.sql("""select * from default.orders
            where order_date between '2013-01-01' and '2013-12-12'
            """)

val orders = spark.sql("select * from orders")
val result = orders.filter($"order_date".between("2013-01-01","2013-12-12"))

orders.write.option("compression","gzip").
        parquet("/user/spark/dataset/result/scenario6/solution")


/* Scenario 07 */
val cat = spark.read.csv("/user/spark/dataset/retail_db/categories").
                toDF("category_id","category_department_id","category_name")

cat.write.format("hive").
        option("compression","uncompressed").
        mode("overwrite").
        saveAsTable("default.categories_replica")
// [hive] describe formatted categories_replica; 


/* Scenario 08 */
spark.sql("""
        CREATE TABLE IF NOT EXISTS default.categories_parquet(
            category_id int,
            category_name string
        ) STORED AS PARQUET""")

val cat = spark.read.csv("/user/spark/dataset/retail_db/categories").
                toDF("category_id","category_department_id","category_name")
cat.select("category_id","category_name").write.mode("overwrite").
        // format("hive").
        saveAsTable("default.categories_parquet")


/* Scenario 09 */
// spark-shell --packages org.apache.spark:spark-avro_2.12:2.4.5
val prod = spark.read.format("avro").
            load("/user/spark/dataset/retail_db/products_avro")

val result = prod.select("product_id","product_price")
result.write.option("compression","none").
        json("/user/spark/dataset/result/scenario9/solution")


/* Scenario 10 */
// spark.sql("""
//         CREATE TABLE IF NOT EXISTS default.categories_partitioned(
//             category_id int,
//             category_department_id int,
//             category_name string)
//         """)

val cat = spark.read.csv("/user/spark/dataset/retail_db/categories").
                toDF("category_id","category_department_id","category_name")

spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

cat.write.partitionBy("category_department_id").
    format("hive").
    saveAsTable("default.categories_partitioned")