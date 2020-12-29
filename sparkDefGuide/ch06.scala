// 다양한 데이터 타입 다루기
import org.apache.spark.sql.functions

val df = spark.read.format("csv").
         option("header","true").
         option("inferSchema","true").
         load("/home/hoon/project/sparkDefGuide/master/data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.createOrReplaceTempView("dfTable")

// 스파크 데이터 타입 다루기
df.select(lit(5),lit("five"),lit(5.0))

// 비교 연산
df.where(col("InvoiceNo").equalTo(536365)).
   select("InvoiceNo","Description").
   show(5,false)
df.where(col("InvoiceNo") === 536365).
   select("InvoiceNo","Description").
   show(5,false)
df.where("InvoiceNo = 536365").
   show(5,false)
df.where("InvoiceNo <> 536365").
   show(5,false)

// 필터 연산
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).
   show()

val DOTCodeFilter = col("StockCode") === "DOT"
df.withColumn("isExpensive",DOTCodeFilter.and(priceFilter.or(descripFilter))).
   where("isExpensive").
   select("UnitPrice","isExpensive").show(5)
df.withColumn("isExpensive",not(col("UnitPrice").leq(250))).
   filter("isExpensive").
   select("Description","UnitPrice").show(5)
df.withColumn("isExpensive",expr("NOT UnitPrice <= 250")).
   filter("isExpensive").
   select("Description","UnitPrice").show(5)

// 수학 함수 연산
val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"),2) + 5
df.select(expr("CustomerId"),fabricatedQuantity.alias("realQuantity")).show(2)
df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
df.select(round(col("UnitPrice"),1).alias("rounded"),col("UnitPrice")).show(5)
df.select(round(lit("2.5")),bround(lit("2.5"))).show(2)

df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show()
df.describe().show()

// 통계 연산
val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05
df.stat.approxQuantile(colName,quantileProbs,relError)

df.stat.crosstab("StockCode","Quantity").show()
df.stat.freqItems(Seq("StockCode","Quantity")).show()

// 기타 연산
df.select(monotonically_increasing_id()).show()

// 문자열 데이터 타입 다루기
df.select(initcap(col("Description"))).show(2,false)
df.select(col("Description"),
   lower(col("Description")),
   upper(lower(col("Description")))).show(2)

df.select(
    ltrim(lit("     HELLO     ")).as("ltrim"),
    rtrim(lit("     HELLO     ")).as("rtrim"),
    trim(lit("     HELLO     ")).as("trim"),
    lpad(lit("HELLO"),3," ").as("lp"),
    rpad(lit("HELLO"),10," ").as("rp")).show(2)

