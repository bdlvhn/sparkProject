val cust = spark.read.json("/user/spark/dataset/retail_db/customers-json")
cust.show(5) // customers 파일 json 형태 읽기

cust.select(concat_ws(" ",$"customer_fname",$"customer_lname")).show(5)
// concat 함수 사용한 결과 확인
cust.select(concat_ws(" ",$"customer_fname",$"customer_lname").alias("customer_name")).show(5)
// alias로 열 이름 변경

cust.select(lower($"customer_fname").alias("first_name")).show(5)
cust.select(upper($"customer_fname").alias("first_name")).show(5)
// first name 소,대문자로 변경

cust.select(regexp_replace($"customer_zipcode","785","999").alias("zipcode")).show(5)
// zipcode 785를 999로 변경

cust.select("customer_street").show(5)
cust.select(split($"customer_street"," ").getItem(0).alias("street_name")).show(5)
// street에서 첫 번째 값만 split해서 확인

cust.select("customer_fname").show(5)
cust.select(substring($"customer_fname",0,4).alias("name")).show(5)
// 이름 중 첫 4자리 substring하여 확인
