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

val simpleColors = Seq("black","white","red","green","blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")
df.select(
  regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
  col("Description")).show(2,false)

df.select(translate(col("Description"),"LEET","1337"), col("Description")).
   show(2,false)

val regexString = simpleColors.map(_.toUpperCase).mkString("(","|",")")
df.select(
  regexp_extract(col("Description"),regexString,1).alias("color_clean"),
  col("Description")).show(2,false)

val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("Description").contains("WHITE")
df.withColumn("hasSimpleColor",containsBlack.or(containsWhite)).
   where("hasSimpleColor").
   select("Description").show(3,false)


val simpleColors = Seq("black","white","red","green","blue")
val selectedColumns = simpleColors.map { color => {
  col("Description").contains(color.toUpperCase).alias(s"is_$color")
}}:+expr("*")

df.select(selectedColumns:_*).where(col("is_white").or(col("is_red"))).
   select("Description").show(3,false)

// 날짜와 타임스탬프 데이터 타입 다루기
// 두 가지 시간 관련 정보; date, timestamp

val dateDF = spark.range(10).
                   withColumn("today",current_date()).
                   withColumn("now",current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

dateDF.select(date_sub(col("today"),5),date_add(col("today"),5)).show(1,false)

dateDF.withColumn("week_ago",date_sub(col("today"),7)).
       select(datediff(col("week_ago"),col("today"))).show(1,false)

dateDF.select(
  to_date(lit("2016-01-01")).alias("start"),
  to_date(lit("2017-05-22")).alias("end")).
  select(months_between(col("start"),col("end"))).show(1,false)

spark.range(5).withColumn("date",lit("2017-01-01")).
  select(to_date(col("date"))).show(1,false)

dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1,false)

val dateFormat = "yyyy-dd-MM"
val cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"),dateFormat).alias("date"),
  to_date(lit("2017-20-12"),dateFormat).alias("date2"))
// cleanDateDF.createOrReplaceTempView("dateTable2")

cleanDateDF.select(to_timestamp(col("date"),dateFormat)).show(false)
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
cleanDateDF.filter(col("date2") > "'2017-12-12'").show()

// null 값 다루기
df.select(coalesce(col("Description"), col("CustomerId"))).show()

df.na.drop()
df.na.drop("any") // 하나라도 null 값을 가지면
df.na.drop("all") // 모든 컬럼이 null이나 NaN일 때
df.na.drop("all",Seq("StockCode","InvoiceNo"))

df.na.fill("All Null values become this string")

df.na.fill(5,Seq("StockCode","InvoiceNo"))

val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
df.na.fill(fillColValues)

df.na.replace("Description", Map("" -> "UNKNOWN"))

df.selectExpr("(Description, InvoiceNo) as complex", "\*")
df.selectExpr("struct(Description, InvoiceNo) as complex", "\*")

// 복합 데이터 타입 다루기
val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))

complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))

df.select(split(col("Description")," ")).show(2)

df.select(split(col("Description")," ").alias("array_col")).
  selectExpr("array_col[0]").show(2)

df.select(size(split(col("Description")," "))).show(2)

df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)

df.withColumn("splitted",split(col("Description")," ")).
   withColumn("exploded",explode(col("splitted"))).
   select("Description","InvoiceNo","exploded").show(5,false)

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2,false)

df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).
   selectExpr("complex_map['WHITE METAL LANTERN']").show(2,false)

df.select(map(col("Description"),col("InvoiceNo")).alias("complex_map")).
  selectExpr("explode(complex_map)").show(2)

// JSON 다루기

val jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1,2,3]}}' as jsonString""")

jsonDF.select(
  get_json_object(col("jsonString"),"$.myJSONKey.myJSONValue[1]") as "column",
  json_tuple(col("jsonString"),"myJSONKey")).show(2)

df.selectExpr("(InvoiceNo, Description) as myStruct").
   select(to_json(col("myStruct")))

import org.apache.spark.sql.types._

val parseSchema = new StructType(Array(
  new StructField("InvoiceNo",StringType,true),
  new StructField("Description",StringType,true)))

df.selectExpr("(InvoiceNo, Description) as myStruct").
   select(to_json(col("myStruct")).alias("newJSON")).
   select(from_json(col("newJSON"),parseSchema), col("newJSON")).show(2)

val udfExampleDF = spark.range(5).toDF("num")
def power3(number:Double):Double = number * number * number
power3(2.0)

spark.udf.register("power3",power3(_:Double):Double)
udfExampleDF.selectExpr("power3(num)").show(2)

