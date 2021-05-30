 Scenario 11 */
// spark-shell --packages org.apache.spark:spark-avro_2.12:2.4.5
val customers = spark.read.format("avro").
                load("/user/spark/dataset/retail_db/customers-avro")

// 1. using scala function
val result = customers.
            select($"customer_id",
                   concat_ws(" ",substring($"customer_fname",1,1),$"customer_lname").alias("customer_name")).
        map(row => row.mkString("\t"))

// 2. using spark sql
customers.createOrReplaceTempView("cust_view")
val result = spark.sql("""
        select customer_id,
        concat_ws(' ',substring(customer_fname,1,1),customer_lname) customer_name
        from cust_view""").
        map(row => row.mkString("\t"))

result.write.option("compression","bzip2").
             text("/user/spark/dataset/result/scenario11/solution")


/* Scenario 12 */
val orders = spark.read.parquet("/user/spark/dataset/retail_db/orders_parquet")

// 1. using scala function
val result = orders.withColumn("order_date",to_date(from_unixtime($"order_date"/1000))).
                    filter($"order_status"==="PENDING" && month($"order_date")===7).
                    select("order_date","order_status")
                    // filter($"order_status"==="PENDING" && $"order_date".like("%2013-07%")).

// 2. using spark sql   
import org.apache.spark.sql._
orders.createOrReplaceTempView("order_view")
val result = spark.sql("""
        select to_date(from_unixtime(order_date/1000)) order_date,
               order_status
        from order_view
        where order_status = 'PENDING'
        and month(to_date(from_unixtime(order_date/1000))) = 7
        // and to_date(from_unixtime(order_date/1000)) like '2013-07%'
        """)       

result.write.option("compression","snappy").
       json("/user/spark/dataset/result/scenario12/solution")


/* Scenario 13 */
val customers = spark.sql("""
                select *
                from default.customers
                where customer_fname like "%Rich%"
                """)

customers.write.option("compression","snappy").
          parquet("/user/spark/dataset/result/scenario13/solution")
// hadoop jar parquet-tools-1.9.0.jar cat --json /user/spark/dataset/result/scenario13/solution/*.snappy.parquet


/* Scenario 14 */
val customers = spark.read.option("delimiter","\t").
                csv("/user/spark/dataset/retail_db/customers-tab-delimited").
                toDF("customer_id","customer_fname","customer_lname","customer_email",
                    "customer_password","customer_street","customer_city","customer_state","customer_zipcode")

// 1. using scala function
customers.filter($"customer_fname".like("M%")).
            groupBy("customer_state").agg(count("*").as("count")).show()

// 2. using spark sql
customers.createOrReplaceTempView("cust_view")
val result = spark.sql("""
            select customer_state, count(1) count
            from cust_view
            where customer_fname like 'M%'
            group by customer_state
            """)

result.write.mode(SaveMode.Overwrite).
            option("compression","gzip").
            parquet("/user/spark/dataset/result/scenario14/solution")


/* Scenario 15 */
val orders = spark.read.csv("/user/spark/dataset/retail_db/orders").toDF("order_id","order_date","order_customer_id","order_status")
val customers = spark.read.option("header",true).csv("/user/spark/dataset/retail_db/customers")

// 1. using scala function
var result = orders.groupBy($"order_customer_id").count()
result = result.filter($"count">5)
result = result.join(customers,result.col("order_customer_id") === customers.col("customer_id")).
                filter(col("customer_fname").like("M%")).
                select("customer_fname","customer_lname","count").
                sort(desc("count"))

result.map(row => row.mkString("|")).
    write.option("compression","gzip").
    text("/user/spark/dataset/result/scenario15/solution")

// 2. using spark sql
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

val result = spark.sql("""
        select customer_id, customer_fname, customer_lname, count(1) count
        from customers c join orders o on c.customer_id = o.order_customer_id
        where customer_fname like 'M%'
        group by customer_id, customer_fname, customer_lname
        having count(1) > 5
        order by count desc
        """).select("customer_fname","customer_lname","count")

import org.apache.spark._
result.map(row => row.mkString("|")).
    write.option("compression","gzip").
    mode(SaveMode.Overwrite).
    text("/user/spark/dataset/result/scenario15/solution")