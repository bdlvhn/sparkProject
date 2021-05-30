// 비지도학습 : 이상 탐지에 유용함. 여기서는 네트워크 이상 탐지 모델 만들어볼 예정


// K-평균 군집화

// ### 5.5 첫 번째 군집화하기
val dataWithoutHeader = spark.read.
    option("inferSchema",true).
    option("header",false).
    csv("/home/hoon/project/sparkAdv/ch05_clustering/data/kddcup.data")

val data = dataWithoutHeader.toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label")


// 레이블 별 갯수 확인
data.select("label").groupBy("label").count().orderBy($"count".desc).show(25)

// 수치형 자료만 유지 후, 어셈블러와 모델 및 파이프라인 구성
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler


val numericOnly = data.drop("protocol_type","service","flag").cache()
numericOnly.count()

val assembler = new VectorAssembler().
    setInputCols(numericOnly.columns.filter(_!="label")).
    setOutputCol("featureVector")

val kmeans = new KMeans().
    setPredictionCol("cluster").
    setFeaturesCol("featureVector")

val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
val pipelineModel = pipeline.fit(numericOnly)
val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]


// Centroid 확인
kmeansModel.clusterCenters.foreach(println)


// 분류 결과 확인
val withCluster = pipelineModel.transform(numericOnly)
withCluster.select("cluster","label").
    groupBy("cluster","label").count().
    orderBy($"cluster",$"count".desc).
    show(25)


// ### 5.6 k 선정하기
import org.apache.spark.sql.DataFrame
import scala.util.Random

def clusteringScore0(data: DataFrame, k: Int): Double = {

    val assembler = new VectorAssembler().
    setInputCols(numericOnly.columns.filter(_!="label")).
    setOutputCol("featureVector")

    val kmeans = new KMeans().
        setSeed(Random.nextLong()).
        setK(k).
        setPredictionCol("cluster").
        setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
    val kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
    kmeansModel.computeCost(assembler.transform(data)) / data.count()
}

(20 to 100 by 20).map(k => (k,clusteringScore0(numericOnly,k))).
    foreach(println)

def clusteringScore1(data: DataFrame, k: Int): Double = {

    val assembler = new VectorAssembler().
    setInputCols(numericOnly.columns.filter(_!="label")).
    setOutputCol("featureVector")

    val kmeans = new KMeans().
        setSeed(Random.nextLong()).
        setK(k).
        setPredictionCol("cluster").
        setFeaturesCol("featureVector").
        setMaxIter(40). // 기본값인 20보다 크게 설정
        setTol(1.0e-5) // 기본값인 1.0e-4보다 작게 설정

    val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
    val kmeansModel = pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
    kmeansModel.computeCost(assembler.transform(data)) / data.count()
}

(20 to 100 by 20).map(k => (k,clusteringScore1(numericOnly,k))).
    foreach(println)


// ### 5.8 특징 정규화
import org.apache.spark.ml.feature.StandardScaler

def clusteringScore2(data: DataFrame, k: Int): Double = {

    val assembler = new VectorAssembler().
    setInputCols(numericOnly.columns.filter(_!="label")).
    setOutputCol("featureVector")

    val scaler = new StandardScaler().
        setInputCol("featureVector").
        setOutputCol("scaledFeatureVector").
        setWithStd(true).
        setWithMean(false)

    val kmeans = new KMeans().
        setSeed(Random.nextLong()).
        setK(k).
        setPredictionCol("cluster").
        setFeaturesCol("featureVector").
        setMaxIter(40). // 기본값인 20보다 크게 설정
        setTol(1.0e-5) // 기본값인 1.0e-4보다 작게 설정

    val pipeline = new Pipeline().setStages(Array(assembler,scaler,kmeans))
    val pipelineModel = pipeline.fit(data)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.computeCost(pipelineModel.transform(data)) / data.count()
}

(60 to 270 by 30).map(k => (k,clusteringScore2(numericOnly,k))).
    foreach(println)


// ### 5.9 범주형 변수
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

def oneHotPipeline(inputCol: Stirng): (Pipeline, String) = {
    
    val indexer = new StringIndexer().
    setInputCol(inputCol).
    setOutputCol(inputCol + "_indexed")

    val encoder = new OneHotEncoder().
    setInputCol(inputCol + "_indexed").
    setOutputCol(inputCol + "_vec")

    val pipeline = new Pipeline().setStages(Array(indexer,encoder))
    (pipeline, inputCol + "_vec")
}

def clusteringScore3(data: DataFrame, k: Int): Double = {
    val (protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
    val (serviceEncoder, serviceVecCol) = oneHotPipeline("service")
    val (flagEncoder, flagVecCol) = oneHotPipeline("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)

    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    val pipelineModel = pipeline.fit(data)

    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    kmeansModel.summary.trainingCost
  }


// ### 5.10 엔트로피와 함께 레이블 활용하기
def entropy(counts: Iterable[Int]): Double = {
    val values = counts.filter(_ > 0)
    val n = values.map(_.toDouble).sum
    values.map { v =>
        val p = v / n
        -p * math.log(p)}.sum
}

val clusterLabel = pipelineModel.transform(data).
    select("cluter","label").as[(Int,String)]

val weightedClusterEntropy = clusterLabel.
    groupByKey { case (cluster,_) => cluster}.
    mapGroups { case (_,clusterLabels) => 
        val labels = clusterLabels.map { case (_,label) => label}.toSeq
        val labelCounts = labels.groupBy(identity).values.map(_.size)
        lables.size * entropy(labelCounts)}.collect()

weightedClusterEntropy.sum / data.count()

// 5.11 군집화하기
def fitPipeline4(data: DataFrame, k: Int): PipelineModel = {
    val (protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
    val (serviceEncoder, serviceVecCol) = oneHotPipeline("service")
    val (flagEncoder, flagVecCol) = oneHotPipeline("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))
    pipeline.fit(data)
  }

// k=180으로 하이퍼파라미터 전달
val pipelineModel = fitPipeline4(data, 180)
val countByClusterLabel = pipelineModel.transform(data).
    select("cluster","label").
    groupBy("cluster","label").count().
    orderBy("cluster","label")
countByClusterLabel.show()

// 문턱값 설정
import org.apache.spark.ml.linalg.{Vector, Vectors}

val kMeansModel = pipelineModel.stages.last.asInstanceOf[kMeansModel]
val centroids = kMeansModel.clusterCenters

val clustered = pipelineModel.transform(data)
val threshold = clustered.
    select("cluster","scaledFeatureVector").as[(Int,Vector)].
    map { case (cluster,vec) => Vectors.sqdist(centroids(cluster),vec)}.
    orderBy($"value".desc).take(100).last

val originalCols = data.columns
val anomalies = clustered.filter { row =>
    val cluster = row.getAs[Int]("cluster")
    val vec = row.getAs[Vector]("scaledFeatureVector")
    Vectors.sqdist(centroids(cluster),vec) >= threshold}.
    select(originalCols.head,originalCols.tail:_*)

anomalies.first()
