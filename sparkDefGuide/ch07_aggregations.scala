// 데이터 준비
val df = spark.read.format("csv").
    option("header","true").
    option("inferSchema","true").
    load("/home/hoon/project/sparkDefGuide/master/data/retail-data/all/*.csv").
    coalesce(5)
df.cache()
df.count() == 541909 // count가 액션이므로 dataFrame 캐싱 작업을 수행하는 용도로도 사용

// # 7.1 집계 함수
import org.apache.spark.sql.functions

// 카운트, 고유값 카운트
df.select(count("StockCode")).show() // 541909
df.select(countDistinct("StockCode")).show() // 4070
df.select(approx_count_distinct("StockCode",0.1)).show() // 3364

// 처음, 마지막 값; 최대, 최소 및 합
df.select(first("StockCode"),last("StockCode")).show()
df.select(min("Quantity"),max("Quantity")).show()
df.select(sum("Quantity")).show()
df.select(sumDistinct("Quantity")).show()

// 평균
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
.selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()

// 분산, 표준편차; samp - 표본, pop - 모
df.select(var_pop("Quantity"),var_samp("Quantity"),
    stddev_pop("Quantity"),stddev_samp("Quantity")).show()

// 비대칭도와 점도 (skewness, kurtosis)
df.select(skewness("Quantity"),kurtosis("Quantity")).show()

// 공분산과 상관관계
df.select(corr("InvoiceNo","Quantity"),covar_samp("InvoiceNo","Quantity"),
    covar_pop("InvoiceNo","Quantity")).show()

// 복합 데이터 타입의 집계
df.agg(collect_set("Country"),collect_list("Country")).show()


// # 7.2 그룹화
df.groupBy("InvoiceNo","CustomerId").count().show()

// 표현식을 통한 그룹화
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

// 맵을 이용한 그룹화
df.groupBy("InvoiceNo").agg("Quantity"->"avg","Quantity"->"stddev_pop").show()

// # 7.3 윈도우 함수
import org.apache.spark.sql.expressions.Window

val dfWithDate = df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
val windowSpec = Window.
    partitionBy("CustomerId","date").
    orderBy(col("Quantity").desc).
    rowsBetween(Window.unboundedPreceding, Window.currentRow)

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").
    select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).
    show()

// # 7.4 그룹화 셋
val dfNoNull = dfWithDate.drop()

// 롤업
val rolledUpDF = dfNoNull.rollup("Date","Country").agg(sum("Quantity")).
    selectExpr("Date","Country","`sum(Quantity)` as total_quantity").
    orderBy("Date")
rolledUpDF.show()
rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()

// 큐브
dfNoNull.cube("Date","Country").agg(sum(col("Quantity"))).
    select("Date","Country","sum(Quantity)").orderBy("Date").show()

// 그룹화 메타데이터
dfNoNull.cube("customerId","stockCode").agg(grouping_id(),sum("Quantity")).
    orderBy(col("grouping_id()").desc).show()

// 피벗
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date","`USA_sum(Quantity)`").show()

// 사용자 정의 집계 함수
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("value",BooleanType) :: Nil)
    def bufferSchema: StructType = StructType(
        StructField("result",BooleanType) :: Nil)
    def dataType: DataType = BooleanType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = true
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2:Row): Unit = {
        buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }
    def evaluate(buffer: Row): Any = {
        buffer(0)
    }
}

val ba = new BoolAnd
spark.udf.register("booland",ba)

spark.range(1).
    selectExpr("explode(array(TRUE, TRUE, TRUE)) as t").
    selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t").
    select(ba(col("t")),expr("booland(f)")).
    show()