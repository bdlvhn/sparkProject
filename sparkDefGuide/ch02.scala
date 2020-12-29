import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.desc


/* import */

val myRange = spark.range(1000).toDF("number")
myRange.count
val divisBy2 = myRange.where("number % 2 = 0")
divisBy2.count

val flightData2015 = spark.
                     read.
                     option("inferSchema","true").
                     option("header","true").
                     csv("/home/hoon/project/sparkDefGuide/ch02/2015-summary.csv")
flightData2015.take(3)
flightData2015.sort("count").explain()

spark.conf.set("spark.sql.shuffle.partitions","5")
flightData2015.sort("count").take(2)

flightData2015.createOrReplaceTempView("flight_data_2015")

val sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY 1
    """)

val dataFrameWay = flightData2015.
                   groupBy('DEST_COUNTRY_NAME).
                   count()

sqlWay.explain
dataFrameWay.explain

spark.sql("SELECT max(count) from flight_data_2015").take(1)


flightData2015.select(max("count")).take(1)

val maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY 2 DESC
    LIMIT 5
""")
maxSql.show()

flightData2015.groupBy("DEST_COUNTRY_NAME").
               sum("count").
               withColumnRenamed("sum(count)","destination_total").
               sort(desc("destination_total")).
               limit(5).
               show()