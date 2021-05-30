spark // sparkSession 확인

/* 1. 헤더 없는 파일 */
val catDF = spark.read.format("csv").load("/user/spark/dataset/retail_db/categories")
catDF.show(5) // 상위 5개 행 확인. column name이 _c0,_c1,_c2로 정해져있지 않음.

val catDF = spark.read.format("csv").load("/user/spark/dataset/retail_db/categories").
            toDF("category_id","category_department_id","category_name")
catDF.show(5) // 상위 5개 행 확인. column name 설정.

catDF.select("category_id","category_name").show(5) // 5개 행, 선택한 2개의 열만 표시



/* 2. 헤더 존재하는 파일 */
val catDF = spark.read.csv("/user/spark/dataset/retail_db/categories-header")
catDF.show(5) // 헤더가 존재하는 형태 확인

val catDF = spark.read.option("header",true).
            csv("/user/spark/dataset/retail_db/categories-header")
catDF.show(5) // 헤더 옵션 추가



/* 3. 스키마 추론 */
val catDF = spark.read.
            option("inferSchema",true).
            option("header",true).
            csv("/user/spark/dataset/retail_db/categories-header")
catDF.printSchema //스키마 출력하여 확인



/* 4. 스키마 생성 후 전달 */
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val mySchema = StructType(Array(
               StructField("cat_id",IntegerType,true), // 세 번째 true/false는 nullable 옵션
               StructField("cat_dep_id",IntegerType,true),
               StructField("cat_name",StringType,true)
               ))
val catDF = spark.read.
            option("header",true).
            schema(mySchema).
            csv("/user/spark/dataset/retail_db/categories-header")
catDF.show(5) // 스키마 전달하여 만든 DF 확인
catDF.printSchema //스키마 출력하여 확인

catDF.show(3)
catDF.write.csv("/user/spark/dataset/output/cat-header") // hdfs 내에 파일 쓰기
spark.read.csv("/user/spark/dataset/output/cat-header").show(5)// write 성공했는지 확인

catDF.write.option("header",true).csv("/user/spark/dataset/output/cat-header")
/* 헤더 추가했으나 이미 파일이 존재하여 오류 발생 */
catDF.write.option("header",true).mode(SaveMode.Overwrite).csv("/user/spark/dataset/output/cat-header")
spark.read.option("header",true).csv("/user/spark/dataset/output/cat-header").show(5)

catDF.write.
      option("header",true).
      mode(SaveMode.Overwrite).
      option("compression","snappy"). // snappy 형태로 압축
      csv("/user/spark/dataset/output/cat-header")
      
