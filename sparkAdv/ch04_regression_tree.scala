// decision tree의 특징 : 이상치에 잘 안 휘둘림, 정규화가 필요 없음

// 4.6 데이터 준비하기
val dataWithoutHeader = spark.read.
    option("inferSchema","true").
    option("header","false").
    csv("/home/hoon/project/sparkAdv/ch04_regression_tree/data/covtype.data")

val colNames = Seq(
    "Elevation", "Aspect", "Slope",
    "Horizontal_Distance_To_Hydrology",
    "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points"
    ) ++ (
        (0 until 4).map(i => s"Wilderness_Area_$i")
    ) ++ (
        (0 until 40).map(i => s"Soil_Type_$i")
    ) ++ Seq("Cover_Type")

val data = dataWithoutHeader.toDF(colNames:_*).
    withColumn("Cover_Type",$"Cover_Type".cast("double"))

data.head

// 데이터 train, test set 분리
val Array(trainData, testData) = data.randomSplit(Array(0.9,0.1))
trainData.cache()
testData.cache()

// vector assembler
import org.apache.spark.ml.feature.VectorAssembler

val inputCols = trainData.columns.filter(_ != "Cover_Type")
val assembler = new VectorAssembler().
    setInputCols(inputCols).
    setOutputCol("featureVector")

val assembledTrainData = assembler.transform(trainData)
assembledTrainData.select("featureVector").show(truncate=false)

// decision tree classifier
import org.apache.spark.ml.classification.DecisionTreeClassifier
import scala.util.Random

val classifier = new DecisionTreeClassifier().
    setSeed(Random.nextLong()).
    setLabelCol("Cover_Type").
    setFeaturesCol("featureVector").
    setPredictionCol("prediction")

// 모델 생성
val model = classifier.fit(assembledTrainData)
println(model.toDebugString)

// 변수별 중요도 확인
model.featureImportances.toArray.zip(inputCols).
    sorted.reverse.foreach(println)

// 예측치
val predictions = model.transform(assembledTrainData)
predictions.select("Cover_Type","prediction","probability").
    show(truncate = false)

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val evaluator = new MulticlassClassificationEvaluator().
    setLabelCol("Cover_Type").
    setPredictionCol("prediction")

// 정확도 평가
evaluator.setMetricName("accuracy").evaluate(predictions)
evaluator.setMetricName("f1").evaluate(predictions)

// Confusion Matrix
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val predictionRDD = predictions.
    select("prediction","Cover_Type").
    as[(Double,Double)].
    rdd

val MulticlassMetrics = new MulticlassMetrics(predictionRDD)
MulticlassMetrics.confusionMatrix

// Confusion Matrix 직접 계산
val confusionMatrix = predictions.
    groupBy("Cover_Type").
    pivot("prediction",(1 to 7)).
    count().
    na.fill(0.0).
    orderBy("Cover_Type")

confusionMatrix.show()

// 정확도 확인하기
import org.apache.spark.sql.DataFrame

def classProbabilities(data: DataFrame): Array[Double] = {
    val total = data.count()
    data.groupBy("Cover_Type").count().
    orderBy("Cover_Type").
    select("count").as[Double].
    map(_ / total).
    collect()
}

val trainPriorProbabilities = classProbabilities(trainData)
val testPriorProbabilities = classProbabilities(testData)
val accuracy = trainPriorProbabilities.zip(testPriorProbabilities).map {
    case (trainProb, testProb) => trainProb * testProb
}.sum

// 4.9 의사 결정 나무 튜닝하기
import org.apache.spark.ml.Pipeline

val inputCols = trainData.columns.filter(_ != "Cover_Type")
val assembler = new VectorAssembler().
    setInputCols(inputCols).
    setOutputCol("featureVector")

val classifier = new DecisionTreeClassifier().
    setSeed(Random.nextLong()).
    setLabelCol("Cover_Type").
    setFeaturesCol("featureVector").
    setPredictionCol("prediction")

val pipeline = new Pipeline().setStages(Array(assembler,classifier))

import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit

val paramGrid = new ParamGridBuilder().
    addGrid(classifier.impurity, Seq("gini","entropy")).
    addGrid(classifier.maxDepth, Seq(1,20)).
    addGrid(classifier.maxBins, Seq(40,300)).
    addGrid(classifier.minInfoGain, Seq(0.0,0.05)).
    build()

val multiclassEval = new MulticlassClassificationEvaluator().
    setLabelCol("Cover_Type").
    setPredictionCol("prediction").
    setMetricName("accuracy")

val validator = new TrainValidationSplit().
    setSeed(Random.nextLong()).    
    setEstimator(pipeline).
    setEvaluator(multiclassEval).
    setEstimatorParamMaps(paramGrid).
    setTrainRatio(0.9)

val validatorModel = validator.fit(trainData)

import org.apache.spark.ml.PipelineModel

val bestModel = validatorModel.bestModel
bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap

val validatorModel = validator.fit(trainData)

val paramsAndMetrics = validatorModel.validationMetrics.
    zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

paramsAndMetrics.foreach { case (metric, params) =>
    println(metric)
    println(params)
    println()}

validatorModel.validationMetrics.max
multiclassEval.evaluate(bestModel.transform(testData))

// 4.10 범주형 특징 다시 살펴보기

import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector

def unencodeOneHot(data: DataFrame): DataFrame = {
    val wildernessCols = (0 until 4).map(i => s"Wilderness_Area_$i").toArray

    val wilderNessAssembler = new VectorAssembler().
        setInputCols(wildernessCols).
        setOutputCol("wilderness")

    val unhotUDF = udf((vec: Vector) => vec.toArray.indexOf(1.0).toDouble)

    val withWilderness = wilderNessAssembler.transform(data).
        drop(wildernessCols:_*).
        withColumn("wilderness", unhotUDF($"wilderness"))

    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray

    val soilAssembler = new VectorAssembler().
    setInputCols(soilCols).
    setOutputCol("soil")

    soilAssembler.transform(withWilderness).
    drop(soilCols:_*).
    withColumn("soil",unhotUDF($"soil"))
}

val unencTrainData = unencodeOneHot(trainData)

import org.apache.spark.ml.feature.VectorIndexer

val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
val assembler = new VectorAssembler().
    setInputCols(inputCols).
    setOutputCol("featureVector")

val indexer = new VectorIndexer().
    setMaxCategories(40).
    setInputCol("featureVector").
    setOutputCol("indexedVector")

val classifier = new DecisionTreeClassifier().
    setSeed(Random.nextLong()).
    setLabelCol("Cover_Type").
    setFeaturesCol("featureVector").
    setPredictionCol("prediction")

val pipeline = new Pipeline().setStages(Array(assembler,indexer,classifier))

// Random Forest Classifier

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel

val unencTrainData = unencodeOneHot(trainData)
val unencTestData = unencodeOneHot(testData)

val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")

val assembler = new VectorAssembler().
  setInputCols(inputCols).
  setOutputCol("featureVector")

val indexer = new VectorIndexer().
  setMaxCategories(40).
  setInputCol("featureVector").
  setOutputCol("indexedVector")

val classifier = new RandomForestClassifier().
  setSeed(Random.nextLong()).
  setLabelCol("Cover_Type").
  setFeaturesCol("indexedVector").
  setPredictionCol("prediction")
  // setImpurity("entropy").
  // setMaxDepth(20).
  // setMaxBins(300)

val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

val paramGrid = new ParamGridBuilder().
  addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
  addGrid(classifier.numTrees, Seq(1, 10)).
  build()

val multiclassEval = new MulticlassClassificationEvaluator().
  setLabelCol("Cover_Type").
  setPredictionCol("prediction").
  setMetricName("accuracy")

val validator = new TrainValidationSplit().
  setSeed(Random.nextLong()).
  setEstimator(pipeline).
  setEvaluator(multiclassEval).
  setEstimatorParamMaps(paramGrid).
  setTrainRatio(0.9)

val validatorModel = validator.fit(unencTrainData)

val bestModel = validatorModel.bestModel

val forestModel = bestModel.asInstanceOf[PipelineModel].
stages.last.asInstanceOf[RandomForestClassificationModel]

println(forestModel.extractParamMap)
println(forestModel.getNumTrees)
forestModel.featureImportances.toArray.zip(inputCols).
    sorted.reverse.foreach(println)

val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
println(testAccuracy)

bestModel.transform(unencTestData.drop("Cover_Type")).select("prediction").show()
