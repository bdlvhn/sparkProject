import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, Metadata}
import org.apache.spark.sql.functions.{col,column,expr,lit,desc,asc}

// 5.1 스키마
// 불러온 파일 스키마 확인
val df = spark.read.format("json").
		 load("/home/hoon/project/sparkDefGuide/master/data/flight-data/json/2015-summary.json")
df.printSchema()

// 스키마 얻기; StructType 형태
df.schema

// 스키마를 만들고 적용하는 예제
val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME",StringType,true),
    StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    StructField("count",LongType, false,Metadata.fromJson("{\"hello\":\"world\"}"))
))

val df = spark.read.format("json").schema(myManualSchema).
         load("/home/hoon/project/sparkDefGuide/master/data/flight-data/json/2015-summary.json")


// 5.2 컬럼과 표현식
col("someColumnName")
column("someColumnName")
// $"myColumn", 'myColumn도 사용 가능

df.col("count")
// *  컬럼은 단지 표현식이다.
// ** 컬럼과 컬럼의 트랜스포메이션은 파싱된 표현식과 동일한 논리적 실행 계획으로 컴파일된다.
expr("(((someCol + 5) * 200) - 6) < otherCol")

spark.read.format("json").
      load("/home/hoon/project/sparkDefGuide/master/data/flight-data/json/2015-summary.json").
      columns

// 5.3 레코드와 로우
df.first()

val myRow = Row("Hello",null,1,false)

myRow(0)
myRow(0).asInstanceOf[String]
myRow.getString(0)
myRow.getInt(2)

// 5.4 DataFrame의 트랜스포메이션
df.createOrReplaceTempView("dfTable")

val myManualSchema = new StructType(Array(
    StructField("some",StringType,true),
    StructField("col",StringType,true),
    StructField("names",LongType, false)))

val myRows = Seq(Row("Hello",null,1L))
val myRDD = spark.sparkContext.parallelize(myRows)
val myDF = spark.createDataFrame(myRDD, myManualSchema)

myDF.show()

df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

// 다양한 열 명시 방법
df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME,
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME")).
    show(2)

// selectExpr 구문
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
df.select(expr("DEST_COUNTRY_NAME AS destination")).alias("DEST_COUNTRY_NAME").show(2)
df.selectExpr("DEST_COUNTRY_NAME AS newColumnName","DEST_COUNTRY_NAME").show(2)

df.selectExpr(
    "*",
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").
    show(2)

df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(2)

// 명시적인 값
df.select(expr("*"), lit(1).as("One")).show(2)

// 컬럼 추가
df.withColumn("numberOne",lit(1)).show(2)
df.withColumn("withinCountry",expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
df.withColumn("destination",expr("DEST_COUNTRY_NAME")).columns

// 컬럼명 변경
df.withColumnRenamed("DEST_COUNTRY_NAME","dest").columns

// 예약 문자와 키워드
val dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`").
    show(2)

dfWithLongColName.select(col("This Long Column-Name")).columns

// 컬럼 제거하기
df.drop("ORIGIN_COUNTRY_NAME").columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME")

// 컬럼 데이터 타입 변경하기
df.withColumn("count2",col("count").cast("string"))

// 로우 필터링하기
df.filter(col("count")<2).show(2)
df.where("count < 2").show(2)
df.filter(col("count")<2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)

// 고유한 로우 얻기
df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().count()
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

// 무작위 샘플 만들기
val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement,fraction,seed).count()

// 임의 분할하기
val dataFrames = df.randomSplit(Array(0.25,0.75),seed)
dataFrames(0).count() > dataFrames(1).count()

// 로우 합치기와 추가하기
val schema = df.schema
val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L)
    )

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDF = spark.createDataFrame(parallelizedRows,schema)

df.union(newDF).
   where("count = 1").
   where($"ORIGIN_COUNTRY_NAME" =!= "United States").
   show()

// 로우 정렬하기
df.sort("count").show(5)
df.orderBy("count","DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"),col("DEST_COUNTRY_NAME")).show(5)
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

spark.read.format("json").
           load("/home/hoon/project/sparkDefGuide/master/data/flight-data/json/2015-summary.json").
           sortWithinPartitions("count")

// 로우 수 제한하기
df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()

// repartition과 coalesce
df.rdd.getNumPartitions
df.repartition(5)
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5,col("DEST_COUNTRY_NAME"))
df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)

// 드라이버로 로우 데이터 수집하기
val collectDF = df.limit(10)
collectDF.take(5)
collectDF.show()
collectDF.show(5,false)
collectDF.collect()
collectDF.toLocalIterator()