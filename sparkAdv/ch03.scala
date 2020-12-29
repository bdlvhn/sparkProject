
// 3.3 데이터 준비하기
// 스파크 MLlib의 ALS 구현에서 ID는 32bit 정수일 때 훨씬 효율적이다
// ID에 Int를 사용하려면 ID가 Int.MaxValue인 2,147,483,647을 넘으면 안 됨

val rawUserArtistData = spark.read.textFile("/home/hoon/project/sparkAdv/ch03/data/user_artist_data.txt")//.repartition(n)
rawUserArtistData.take(5).foreach(println)

val userArtistDF = rawUserArtistData.map { line => 
    val Array(user,artist,_*) = line.split(" ")
    (user.toInt,artist.toInt)
    }.toDF("user","artist")

userArtistDF.agg(
    min("user"),max("user"),min("artist"),max("artist")).show()

val rawArtistData = spark.read.textFile("/home/hoon/project/sparkAdv/ch03/data/artist_data.txt")

// rawArtistData.map { line =>
//     val (id,name) = line.span(_ != "\t")
//     (id.toInt,name.trim)
//     }.count()

val artistByID = rawArtistData.flatMap { line =>
    val (id,name) = line.span(_ != '\t')
    if (name.isEmpty) {
        None
    } else {
        try {
            Some((id.toInt,name.trim))
        } catch {
            case _: NumberFormatException => None
        }
    }
}.toDF("id","name")

val rawArtistAlias = spark.read.textFile("/home/hoon/project/sparkAdv/ch03/data/artist_alias.txt")
val artistAlias = rawArtistAlias.flatMap { line =>
    val Array(artist,alias) = line.split('\t')
    if (artist.isEmpty){
        None
    } else {
        Some((artist.toInt,alias.toInt))
        }
    }.collect().toMap

artistAlias.head
artistByID.filter($"id" isin (1208690,1003926)).show()

// 3.4 첫 번째 모델 만들기

import org.apache.spark.sql._
import org.apache.spark.broadcast._

def buildCounts(
    rawUserArtistData: Dataset[String],
    bArtistAlias: Broadcast[Map[Int,Int]]): DataFrame = {

    rawUserArtistData.map { line =>
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID =
        bArtistAlias.value.getOrElse(artistID, artistID)
        (userID, finalArtistID, count)
    }.toDF("user","artist","count")
}

val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
val trainData = buildCounts(rawUserArtistData, bArtistAlias)
trainData.cache()

import org.apache.spark.ml.recommendation._
import scala.util.Random

val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(10).
            setRegParam(0.01).
            setAlpha(1.0).
            setMaxIter(5).
            setUserCol("user"). 
            setItemCol("artist").
            setRatingCol("count").
            setPredictionCol("prediction").
            fit(trainData)

model.userFactors.show(1, truncate = false)

// 3.5 추천 결과 추출 검사하기

val userID = 2093760 // userID 하나로 예시

val existingArtistIDs = trainData.
                        filter($"user" === userID).
                        select("artist").as[Int].collect()

artistByID.filter($"id" isin (existingArtistIDs:_*)).show()

def makeRecommendations( // 아티스트 선호도 점수화 및 상위 점수 반환 함수
    model: ALSModel,
    userID: Int,
    howMany: Int): DataFrame = {
    val toRecommend = model.itemFactors.
    select($"id".as("artist")).
    withColumn("user",lit(userID))

    model.transform(toRecommend).
          select("artist","prediction").
          orderBy($"prediction".desc).
          limit(howMany)
}

val topRecommendations = makeRecommendations(model, userID, 5)
topRecommendations.show()

val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
artistById.filter($"id" isin (recommendedArtistIDs:_*)).show()

// 3.7 AUC 계산하기

import scala.collection.mutable.ArrayBuffer

def areaUnderCurve( // auc 코드는 소스에서 가져옴
      positiveData: DataFrame,
      bAllArtistIDs: Broadcast[Array[Int]],
      predictFunction: (DataFrame => DataFrame)): Double = {

    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive".
    // Make predictions for each of them, including a numeric score
    val positivePredictions = predictFunction(positiveData.select("user", "artist")).
      withColumnRenamed("prediction", "positivePrediction")

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other artists, excluding those that are "positive" for the user.
    val negativeData = positiveData.select("user", "artist").as[(Int,Int)].
      groupByKey { case (user, _) => user }.
      flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        // Make at most one pass over all artists to avoid an infinite loop.
        // Also stop when number of negative equals positive set size
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          // Only add new distinct IDs
          if (!posItemIDSet.contains(artistID)) {
            negative += artistID
          }
          i += 1
        }
        // Return the set with user ID added back
        negative.map(artistID => (userID, artistID))
      }.toDF("user", "artist")

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData).
      withColumnRenamed("prediction", "negativePrediction")

    // Join positive predictions to negative predictions by user, only.
    // This will result in a row for every possible pairing of positive and negative
    // predictions within each user.
    val joinedPredictions = positivePredictions.join(negativePredictions, "user").
      select("user", "positivePrediction", "negativePrediction").cache()

    // Count the number of pairs per user
    val allCounts = joinedPredictions.
      groupBy("user").agg(count(lit("1")).as("total")).
      select("user", "total")
    // Count the number of correctly ordered pairs per user
    val correctCounts = joinedPredictions.
      filter($"positivePrediction" > $"negativePrediction").
      groupBy("user").agg(count("user").as("correct")).
      select("user", "correct")

    // Combine these, compute their ratio, and average over all users
    val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
      select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
      agg(mean("auc")).
      as[Double].first()

    joinedPredictions.unpersist()

    meanAUC
  }

val allData = buildCounts(rawUserArtistData, bArtistAlias)
val Array(trainData, cvData) = allData.randomSplit(Array(0.9,0.1))
trainData.cache()
cvData.cache()

val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(10).setRegParam(0.01).setAlpha(1.0).setMaxIter(5).
            setUserCol("user").  setItemCol("artist").
            setRatingCol("count"). setPredictionCol("prediction").
            fit(trainData)

areaUnderCurve(cvData, bAllArtistIDs, model.transform)

def predictMostListened(train: DataFrame)(allData: DataFrame) = {

    val listenCounts = train.
                       groupBy("artist").
                       agg(sum("count").as("prediction")).
                       select("artist","prediction")

    allData.
    join(listenCounts, Seq("artist"),"left_outer").
    select("user","artist","prediction")
}

areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))

val evaluations =
    for (rank <- Seq(5,30);
         regParam <- Seq(4.0,0.00001);
         alpha <- Seq(1.0,40.0))
    yield {
        val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(10).setRegParam(0.01).setAlpha(1.0).setMaxIter(5).
            setUserCol("user").  setItemCol("artist").
            setRatingCol("count"). setPredictionCol("prediction").
            fit(trainData)

    val auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)

    model.userFactors.unpersist()
    model.itemFactors.unpersist()

    (auc, (rank, regParam, alpha))
}

evaluations.sorted.reverse.foreach(println)

val someUsers = allData.select("user").as[Int].distinct().take(100)
val someRecommendations =
    someUsers.map(userID => (userID,makeRecommendations(model,userID,5)))
someRecommendations.foreach { case (userID,recsDF) =>
    val recommendedArtists = recsDF.select("artist").as[Int].collect()
    println(s"$userID -> ${recommendedArtists.mkString(", ")}")
}

// rawUserArtistData.map { line =>
//     val userID = tokens(1).toInt
//     val artistID = tokens(0).toInt
//     }