/* Scenario 16 */
val orders = spark.read.csv("/user/spark/dataset/retail_db/orders").
            toDF("order_id","order_date","order_customer_id","order_status")

// 1. using scala function
val result = orders.filter($"order_status"==="SUSPECTED_FRAUD").
                    withColumn("order_date",substring($"order_date",1,7)).
                    groupBy("order_date").
                    agg(count("*").as("count")).
                    sort(desc("order_date"))

// 2. using spark sql
orders.createOrReplaceTempView("orders")
val result = spark.sql("""
        select substring(order_date,1,7) order_date, count(*) count
        from orders
        where order_status = 'SUSPECTED_FRAUD'
        group by 1
        order by 1 desc
        """)

result.write.option("compression","snappy").
        mode(SaveMode.Overwrite).
        parquet("/user/spark/dataset/result/scenario16/solution")


/* Scenario 17 */
val prod = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/products").
           toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")
val cat = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/categories").
           toDF("category_id","category_department_id","category_name")

// 1. using scala function
val result = prod.join(cat,prod.col("product_category_id")===cat.col("category_id")).
             groupBy($"category_name").
             agg(round(min($"product_price"),2).as("min_price"),
                 round(max($"product_price"),2).as("max_price"),
                 round(avg($"product_price"),2).as("avg_price"))

// 2. using spark sql
prod.createOrReplaceTempView("prod")
cat.createOrReplaceTempView("cat")

val result = spark.sql("""
        select category_name,
               round(min(product_price),2) min_price,
               round(max(product_price),2) max_price,
               round(avg(product_price),2) avg_price
        from prod p join cat c on p.product_category_id = c.category_id
        group by 1
        """)

result.write.option("compression","deflate").
        mode(SaveMode.Overwrite).
        json("/user/spark/dataset/result/scenario17/solution")


/* Scenario 18 */
val orders = spark.read.option("inferSchema",true).
            csv("/user/spark/dataset/retail_db/orders").
            toDF("order_id","order_date","order_customer_id","order_status")
            // .withColumn("order_date",$"order_date".cast("string"))
val customers = spark.read.option("inferSchema",true).
            csv("/user/spark/dataset/retail_db/customers").
            toDF("customer_id","customer_fname","customer_lname","customer_email",
                    "customer_password","customer_street","customer_city","customer_state","customer_zipcode").
            withColumn("customer_id",$"customer_id".cast("integer"))

// 1. using scala function
val result = customers.join(orders,customers.col("customer_id")===orders.col("order_customer_id")).
            filter($"order_status"==="COMPLETE" && year($"order_date")==="2014").
            groupBy("customer_id","customer_fname","customer_lname").
            agg(count("*").as("orders_count")).
            select("customer_fname","customer_lname","orders_count").
            sort(desc("orders_count"))

// 2. using spark sql
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")

val result = spark.sql("""
        select customer_id, customer_fname, customer_lname, count(*) orders_count
        from customers c join orders o on c.customer_id = o.order_customer_id
        where 1=1
        and order_status = 'COMPLETE'
        and order_date like '2014%'
        group by 1,2,3
        order by 4 desc
        """).select("customer_fname","customer_lname","orders_count")

result.write.option("compression","none").
        mode(SaveMode.Overwrite).
        orc("/user/spark/dataset/result/scenario18/solution")


/* Scenario 19 */
val prod = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/products").
           toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")
val cat = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/categories").
           toDF("category_id","category_department_id","category_name")
val ord_items = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/order_items").
           toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

// 1. using scala function
val result = prod.join(cat,prod.col("product_category_id")===cat.col("category_id")).
                  join(ord_items,prod.col("product_id")===ord_items.col("order_item_product_id")).
                  filter($"category_name"==="Accessories").
                  groupBy("product_id","category_name","product_name").
                  agg(round(sum("order_item_subtotal"),2).as("product_revenue")).
                  select("category_name","product_name","product_revenue").
                  sort(desc("product_revenue")).
                  limit(5)

// 2. using spark sql
prod.createOrReplaceTempView("prod")
cat.createOrReplaceTempView("cat")
ord_items.createOrReplaceTempView("ord_items")

val result = spark.sql("""
        select product_id, category_name, product_name, round(sum(order_item_subtotal),2) product_revenue
        from prod p join cat c on p.product_category_id = c.category_id
                    join ord_items o on p.product_id = o.order_item_product_id
        where category_name = 'Accessories'
        group by 1,2,3
        order by 4 desc
        limit 5
        """).select("category_name","product_name","product_revenue")

result.write.option("delimiter","|").
            text("/user/spark/dataset/result/scenario19/solution")


/* Scenario 20 */
val orders = spark.read.option("inferSchema",true).
            csv("/user/spark/dataset/retail_db/orders").
            toDF("order_id","order_date","order_customer_id","order_status")
val customers = spark.read.option("inferSchema",true).
            csv("/user/spark/dataset/retail_db/customers").
            toDF("customer_id","customer_fname","customer_lname","customer_email",
                    "customer_password","customer_street","customer_city","customer_state","customer_zipcode").
            withColumn("customer_id",$"customer_id".cast("integer"))
val ord_items = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/order_items").
           toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")
val prod = spark.read.option("inferSchema",true).
           csv("/user/spark/dataset/retail_db/products").
           toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")

// 1. using scala function

// 2. using spark sql
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")
ord_items.createOrReplaceTempView("ord_items")

val result = spark.sql("""
        select order_id, customer_fname, customer_lname, sum(order_item_subtotal) order_revenue
        from customers c
        join orders o on c.customer_id = o.order_customer_id
        join ord_items oi on o.order_id = oi.order_item_order_id
        where order_status = 'CLOSED'
        group by 1,2,3
        having order_revenue > 500
        """).select("customer_fname","customer_lname","order_revenue")

result.write.option("compression","snappy").
        parquet("/user/spark/dataset/result/scenario20/solution")