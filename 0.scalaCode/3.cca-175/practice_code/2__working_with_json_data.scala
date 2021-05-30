val custData = spark.read.json("/user/spark/dataset/retail_db/customers-json")
custData.show(5) // cust_data 불러오기
custData.select("customer_fname","customer_lname").show(5) // 두 열만 선택하여 보기

val multilineJson = spark.read.json("/user/spark/dataset/retail_db/customers-multiline-json")
multilineJson.show(2)
/* multiline이라서 오류 발생 */
val multilineJson = spark.read.
                    option("multiline",true).
                    json("/user/spark/dataset/retail_db/customers-multiline-json")
multilineJson.show(5)

multilineJson.write.json("/user/spark/dataset/output/multiline-json-op") // json 파일로 출력

custData.show(5)
custData.write.option("compression","gzip"). // gzip 형태로 압축하여 저장
               json("/user/spark/dataset/output/cust-data-gzip")
