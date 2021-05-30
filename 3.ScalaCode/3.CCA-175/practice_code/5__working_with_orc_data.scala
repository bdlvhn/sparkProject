val prodDF = spark.read.orc("/user/spark/dataset/retail_db/products_orc")
prodDF.show(5) // orc 파일 읽기

prodDF.write.orc("/user/spark/dataset/output/prodORC") // orc 파일 쓰기
