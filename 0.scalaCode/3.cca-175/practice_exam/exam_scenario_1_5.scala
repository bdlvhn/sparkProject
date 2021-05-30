/* Scenario 01 */
val custDF = spark.read.option("inferSchema",true).
                   option("delimiter","\t").
                   csv("/user/spark/dataset/retail_db/customers-tab-delimited").
                   toDF("customer_id","customer_fname","customer_lname","customer_email",
                    "customer_password","customer_street","customer_city","customer_state","customer_zipcode")

// 1. using scala function
val result = custDF.filter($"customer_state"==="CA").
            select(concat_ws(" ",$"customer_fname",$"customer_lname").alias("customer_name"))
result.write.text("/user/spark/dataset/result/scenario1/solution")


// 2. using spark sql
custDF.createOrReplaceTempView("cust_view")
val result = spark.sql("""
        select concat_ws(' ',customer_fname,customer_lname) customer_name
        from cust_view
        where customer_state = 'CA'
        """)\

import org.apache.spark.sql._
result.write.mode(SaveMode.Overwrite).
    text("/user/spark/dataset/result/scenario1/solution")


/* Scenario 02 */
val orders = spark.read.parquet("/user/spark/dataset/retail_db/orders_parquet")

// 1. using scala function
val result = orders.filter($"order_status"==="COMPLETE").
        withColumn("order_date",to_date(from_unixtime($"order_date"/1000))).
        select("order_id","order_date","order_status")

result.write.option("compression","gzip").json("/user/spark/dataset/result/scenario2/solution")

// 2. using spark sql
orders.createOrReplaceTempView("orders_view")
val result = spark.sql("""
        select order_id,
               to_date(from_unixtime(order_date/1000)) order_date,
               order_status
        from orders_view
        where order_status = 'COMPLETE'
        """)

import org.apache.spark.sql._
result.write.option("compression","gzip").
        mode(SaveMode.Overwrite).
        json("/user/spark/dataset/result/scenario2/solution")


/* Scenario 03 */
val customers = spark.read.option("delimiter","\t").
            csv("/user/spark/dataset/retail_db/customers-tab-delimited").
            toDF("customer_id","customer_fname","customer_lname","customer_email",
                    "customer_password","customer_street","customer_city","customer_state","customer_zipcode")

// 1. using scala function
val result = customers.filter($"customer_city"==="Caguas")
result.write.option("compression","snappy").
        orc("/user/spark/dataset/result/scenario3/solution")

// 2. using spark sql
customers.createOrReplaceTempView("cust_view")
val result = spark.sql("""
        select *
        from cust_view
        where customer_city = 'Caguas'
    """)

import org.apache.spark.sql._
result.write.mode(SaveMode.Overwrite).
        option("compression","snappy").
        orc("/user/spark/dataset/result/scenario3/solution")


/* Scenario 04 */
val categories = spark.read.csv("/user/spark/dataset/retail_db/categories").
                toDF("category_id","category_department_id","category_name")
val result = categories.map(row => row.mkString("\t"))
// result.show(5,false)
result.write.option("compression","lz4").
        text("/user/spark/dataset/result/scenario4/solution")
// hive --orcfiledump {{filePath}}
// hive --orcfiledump -d {{filePath}} > {{fileName.txt}}


/* Scenario 05 */
// spark-shell --packages org.apache.spark:spark-avro_2.12:2.4.5
val prod = spark.read.format("avro").
            load("/user/spark/dataset/retail_db/products_avro")

// 1. using scala function
val result = prod.filter($"product_price">1000.0)
result.write.option("compression","snappy").
        parquet("/user/spark/dataset/result/scenario5/solution")

// 2. using spark sql
prod.createOrReplaceTempView("prod_view")
val result = spark.sql("""
        select *
        from prod_view
        where product_price > 1000.0
        """)

import org.apache.spark.sql._
result.write.mode(SaveMode.Overwrite).
        option("compression","snappy").
        parquet("/user/spark/dataset/result/scenario5/solution")
