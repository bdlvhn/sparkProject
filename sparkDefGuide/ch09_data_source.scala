spark.read.format("csv").
    option("mode","FAILFAST").
    option("inferSchema","true").
    option("path","path/to/file(s)").
    schema(someSchema).
    load()

spark.read.format("csv").
    option("mode","OVERWRITE").
    option("dateFormat","yyyy-MM-dd").
    option("path","path/to/file(s)").
    save()

spark.read.format("csv").
    option("header","true").
    option("mode","FAILFAST").
    option("inferSchema","true").
    load("some/path/to/file.csv")

import org.apache.spark.sql.types._

val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME",StringType,true),
    new StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    new StructField("count",LongType,false)
))

spark.read.format("csv").
    option("header","true").
    option("mode","FAILFAST").
    schema(myManualSchema).
    load("/home/hoon/project/sparkDefGuide/master/data/flight-data/csv/2010-summary.csv").
    show(5)

// CSV 파일
val csvFile = spark.read.format("csv").
    option("header","true").option("mode","FAILFAST").schema(myManualSchema).
    load("/home/hoon/project/sparkDefGuide/master/data/flight-data/csv/2010-summary.csv")

csvFile.write.format("csv").mode("overwrite").option("sep","\t").
    save("/home/hoon/project/sparkDefGuide/tmp/my-tsv-file.tsv")

// JSON 파일
spark.read.format("json").option("mode","FAILFAST").schema(myManualSchema).
    load("/home/hoon/project/sparkDefGuide/master/data/flight-data/json/2010-summary.json").show(5)

csvFile.write.format("json").mode("overwrite").save("/home/hoon/project/sparkDefGuide/tmp/my-json-file.json")

// Parquet 파일
spark.read.format("parquet").
    load("/home/hoon/project/sparkDefGuide/master/data/flight-data/parquet/2010-summary.parquet").show(5)

csvFile.write.format("parquet").mode("overwrite").save("/home/hoon/project/sparkDefGuide/tmp/my-parquet-file.parquet")

// ORC 파일
spark.read.format("orc").
    load("/home/hoon/project/sparkDefGuide/master/data/flight-data/orc/2010-summary.orc").show(5)

csvFile.write.format("orc").mode("overwrite").save("/home/hoon/project/sparkDefGuide/tmp/my-orc-file.orc")

// SQL DB
val driver = "org.sqlite.JDBC"
val path = "/home/hoon/project/sparkDefGuide/master/data/flight-data/jdbc/my-sqlite.db"
val url = s"jdbc:sqlite:/${path}"
val tablename = "flight_info"

// MySQL의 경우 다음과 같이 테스트해볼 수 있음
// import java.sql.DriverManager

// val connection = DriverManager.getConnection(url)
// connection.isClosed()
// connection.close()

// val dbDataFrame = spark.read.format("jdbc").option("url",url).
//     option("dbtable",tablename).option("driver",driver).load()


// Postgre SQL 확인
// val pgDF = spark.read.format("jdbc").
//     option("driver","org.postgresql.Driver").
//     option("url","jdbc:postgresql://database_server").
//     option("dbtable","schema.tablename").
//     option("user","username").option("password","my-secret-password").load()

dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain

dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla','Sweden')").explain

val pushDownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
    AS flight_info"""

val dbDataFrame = spark.read.format("jdbc").
    option("url",url).option("dbtable",pushDownQuery).option("driver",driver).
    load()

dbDataFrame.explain()

val dbDataFrame = spark.read.format("jdbc").
    option("url",url).option("dbtable",tablename).option("driver",driver).
    option("numPartitions",10).load()

dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show()

val props = new java.util.Properties
props.setProperty("driver","org.sqlite.JDBC")
val predicates = Array(
    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
spark.read.cdbc(url, tablename, predicates, props).show()
spark.read.cdbc(url, tablename, predicates, props).rdd.getNumPartitions // 2

val props = new java.util.Properties
props.setProperty("driver","org.sqlite.JDBC")
val predicates = Array(
    "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
    "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
spark.read.jdbc(url, tablename, predicates, props).count() // 510

val colName = "count"
val lowerBound = 0L
val upperBound = 348113L
val numPartitions = 10

spark.read.jdbc(url,tablename,colName,lowerBound,upperBound,numPartitions,props).
    count() // 255

// SQL DB 쓰기
val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.mode("overwrite").jdbc(newPath, tablename, props)
spark.read.jdbc(url, tablename, props).count() // 255

csvFile.write.mode("append").jdbc(newPath, tablename, props)
spark.read.jdbc(url, tablename, props).count() // 765

// Text File
spark.read.textFile("/home/hoon/project/sparkDefGuide/master/data/flight-data/csv/2010-summary.csv").
    selectExpr("split(value, ',' as rows").show()

// 텍스트 파일을 쓸 때는 문자열 컬럼이 하나만 존재해야 한다. 그렇지 않으면 실패함.
csvFile.select("DEST_COUNTRY_NAME").write.text("/home/hoon/project/sparkDefGuide/tmp/simple-text-file.txt")

csvFile.limit(10).select("DEST_COUNTRY_NAME","count").
    write.partitionBy("count").text("/home/hoon/project/sparkDefGuide/tmp/five-csv-files2.csv")

// 고급 I/O
// 병렬로 데이터 쓰기 (파티셔닝)
csvFile.repartition(5).write.format("csv").save("/home/hoon/project/sparkDefGuide/tmp/multiple.csv")

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME").
    save("/home/hoon/project/sparkDefGuide/tmp/partitioned-files.parquet")

// 병렬로 데이터 쓰기 (버켓팅)
val numberBuckets = 10
val columnToBucketBy = "count"

csvFile.write.format("parquet").mode("overwrite").
    bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")

df.write.option("maxRecordsPerFile",5000)