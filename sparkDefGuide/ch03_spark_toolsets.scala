// ### 3-2 ###

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)
val flightsDF = spark.read.
                parquet("/home/hoon/project/sparkDefGuide/master/data/flight-data/parquet/2010-summary.parquet/")

val flights = flightsDF.as[Flight]


val staticDataFrame = spark.read.format("csv").
                      option("header","true").
                      option("inferSchema","true").
                      load("/home/hoon/project/sparkDefGuide/master/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema

// ### 3-3 ###
// 동작 방식 확인

spark.conf.set("spark.sql.shuffle.partitions","5")

import org.apache.spark.sql.functions.{window,col}

staticDataFrame.selectExpr(
                "CustomerId",
                "(UnitPrice*Quantity) as total_cost",
                "InvoiceDate").
                groupBy(
                    col("CustomerId"),window(col("InvoiceDate"), "1 day")).
                sum("total_cost").
                show(5)

// 스트리밍 방식 확인
val streamingDataFrame = spark.readStream.
                         schema(staticSchema).
                         option("maxFilesPerTrigger",1).
                         format("csv").
                         option("header","true").
                         load("/home/hoon/project/sparkDefGuide/master/data/retail-data/by-day/*.csv")

streamingDataFrame.isStreaming

val purchaseByCustomerPerHour = streamingDataFrame.
                                selectExpr(
                                "CustomerId",
                                "(UnitPrice*Quantity) as total_cost",
                                "InvoiceDate").
                                groupBy(
                                col("CustomerId"),window(col("InvoiceDate"),"1 day")).
                                sum("total_cost")

purchaseByCustomerPerHour.writeStream.
                          format("memory").
                          queryName("customer_purchases").
                          outputMode("complete").
                          start()

spark.sql("""
        SELECT *
        FROM customer_purchases
        ORDER BY `sum(total_cost)` DESC
        """).
     show(5)

purchaseByCustomerPerHour.writeStream.
                          format("console").
                          queryName("customer_purchases_2").
                          outputMode("complete").
                          start()

// ### 3-4 ###
staticDataFrame.printSchema()

import org.apache.spark.sql.functions.date_format

val preppedDataFrame = staticDataFrame.
                       na.fill(0).
                       withColumn("day_of_week",date_format($"InvoiceDate","EEEE")).
                       coalesce(5)

val trainDataFrame = preppedDataFrame.
                     where("InvoiceDate < '2011-07-01'")
val testDataFrame = preppedDataFrame.
                     where("InvoiceDate >= '2011-07-01'")

trainDataFrame.count()
testDataFrame.count()

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

val indexer = new StringIndexer().
              setInputCol("day_of_week").
              setOutputCol("day_of_week_index")

val encoder = new OneHotEncoder().
              setInputCol("day_of_week_index").
              setOutputCol("day_of_week_encoded")

val vectorAssembler = new VectorAssembler().
                      setInputCols(Array("UnitPrice","Quantity","day_of_week_encoded")).
                      setOutputCol("features")

val transformationPipeline = new Pipeline().
                             setStages(Array(indexer,encoder,vectorAssembler))

val fittedPipeline = transformationPipeline.fit(trainDataFrame)
val transformedTraining = fittedPipeline.transform(trainDataFrame)

transformedTraining.cache()

val kmeans = new KMeans().
             setK(20).
             setSeed(1L)

val kmModel = kmeans.fit(transformedTraining)
kmModel.computeCost(transformedTraining)

val transformedTest = fittedPipeline.transform(testDataFrame)
kmModel.computeCost(transformedTest)

// ### 3-5 ###

spark.sparkContext.parallelize(Seq(1,2,3)).toDF()

// ### 3-6 ###
