// GraphX로 동시발생 네트워크 분석하기
// 7.2 데이터 구하기

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.conf.Configuration

def loadMedline(spark:SparkSession, path:String) = {
    import spark.implicits._
    @transient val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val sc = spark.sparkContext
    val in = sc.newAPIHadoopFile(path, classOf[XMLInputFormat],
        classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString).toDS()
}
val medlineRaw = loadMedline(spark, "/home/hoon/project/sparkAdv/ch07_graphX/data")

// 7.3 스칼라 XML 라이브러리로 XML 문서 파싱하기
import scala.xml._

val cit = <MedlineCitation>data</MedlineCitation>
val rawXml = medlineRaw.take(1)(0)
val elem = XML.loadString(rawXml)

elem.label
elem.attributes

elem \ "MeshHeadingList"
elem \\ "MeshHeading"
(elem \\ "DescriptorName").map(_.text)

def majorTopics(record: String): Seq[String] = {
    val elem = XML.loadString(record)
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text)
}
majorTopics(rawXml)

val medline = medlineRaw.map(majorTopics)
medline.cache()
medline.take(1)(0)

// 7.4 MeSH 주요 주제와 주제들의 동시발생 분석하기
medline.count()
val topics = medline.flatMap(mesh => mesh).toDF("topic")
topics.createOrReplaceTempView("topics")
val topicDist = spark.sql("""
    SELECT topic, count(*) cnt
    FROM topics
    GROUP BY topic
    ORDER BY cnt DESC""")
topicDist.count()
topicDist.show()

topicDist.createOrReplaceTempView("topic_dist")
spark.sql("""
    SELECT cnt, count(*) dist
    FROM topic_dist
    GROUP BY cnt
    ORDER BY dist DESC
    LIMIT 10""").show()

val list = List(1,2,3)
val combs = list.combinations(2)
combs.foreach(println)

val combs = list.reverse.combinations(2)
combs.foreach(println)
List(3,2) == List(2,3)

val topicPairs = medline.flatMap(t => {
    t.sorted.combinations(2)
}).toDF("pairs")
topicPairs.createOrReplaceTempView("topic_pairs")
val cooccurs = spark.sql("""
    SELECT pairs, count(*) cnt
    FROM topic_pairs
    GROUP BY pairs""")
cooccurs.cache()
cooccurs.count()

cooccurs.createOrReplaceTempView("cooccurs")
spark.sql("""
    SELECT pairs, cnt
    FROM cooccurs
    ORDER BY cnt DESC
    LIMIT 10""").collect().foreach(println)

// 7.5 그래프엑스로 동시발생 네트워크 구성하기

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

def hashId(str: String): Long = {
    val bytes = MessageDigest.getInstance("MD5").
    digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
    ((bytes(1) & 0xFFL) << 8) |
    ((bytes(2) & 0xFFL) << 16) |
    ((bytes(3) & 0xFFL) << 24) |
    ((bytes(4) & 0xFFL) << 32) |
    ((bytes(5) & 0xFFL) << 40) |
    ((bytes(6) & 0xFFL) << 48) |
    ((bytes(7) & 0xFFL) << 56)
}

import org.apache.spark.sql.Row
val vertices = topics.map{ case Row(topic: String) =>
    (hashId(topic), topic) }.toDF("hash","topic")
val uniqueHashes = vertices.agg(countDistinct("hash")).take(1)

import org.apache.spark.graphx._

val edges = cooccurs.map{ case Row(topics: Seq[_], cnt: Long) =>
    val ids = topics.map(_.toString).map(hashId).sorted
    Edge(ids(0), ids(1), cnt)
}

val vertexRDD = vertices.rdd.map{
    case Row(hash: Long, topic: String) => (hash, topic)
}
val topicGraph = Graph(vertexRDD,edges.rdd)
topicGraph.cache()

vertexRDD.count()
topicGraph.vertices.count()

// 7.6 네트워크의 구조 이해하기

val connectedComponentGraph = topicGraph.connectedComponents()

val componentDF = connectedComponentGraph.vertices.toDF("vid","cid")
val componentCounts = componentDF.groupBy("cid").count()
componentCounts.count()
componentCounts.orderBy(desc("count")).show()

val topicComponentDF = topicGraph.vertices.innerJoin(
    connectedComponentGraph.vertices) {
    (topicId, name, componentId) => (name, componentId.toLong)
}.toDF("topic","cid")

topicComponentDF.where("cid._2 = -2062883918534425492").show(false)

val campy = spark.sql("""
    SELECT *
    FROM topic_dist
    WHERE topic LIKE '%ampylobacter%'""")
campy.show()

val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
degrees.map(_._2).stats()

val sing = medline.filter(x => x.size == 1)
sing.count()
val singTopic = sing.flatMap(topic => topic).distinct()
singTopic.count()

val topic2 = topicPairs.flatMap(_.getAs[Seq[String]](0))
singTopic.except(topic2).count()

val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
    (topicId, degree, name) => (name, degree.toInt)
}.values.toDF("topic", "degree")
namesAndDegrees.orderBy(desc("degree")).show()

// 7.7 관련성 낮은 단계 필터링하기
val T = medline.count()
val topicDistRdd = topicDist.map{
    case Row(topic: String, cnt: Long) => (hashId(topic), cnt)
}.rdd

val topicDistGraph = Graph(topicDistRdd,topicGraph.edges)

def chiSq(YY: Long, YB: Long, YA: Long, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY*NN - YN*NY) - T / 2.0
    T * math.pow(inner,2) / (YA*NA*YB*NB)
}

val chiSquaredGraph = topicDistGraph.mapTriplets(triplet => {
    chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
})
chiSquaredGraph.edges.map(x => x.attr).stats()

val interesting = chiSquaredGraph.subgraph(
    triplet => triplet.attr > 19.5)
interesting.edges.count

val interestingComponentGraph = interesting.connectedComponents()
val icDF = interestingComponentGraph.vertices.toDF("vid","cid")
val icCountDF = icDF.groupBy("cid").count()
icCountDF.count()

icCountDF.orderBy(desc("count")).show()

val interestingDegrees = interesting.degrees.cache()
interestingDegrees.map(_._2).stats()

interestingDegrees.innerJoin(topicGraph.vertices) {
    (topicId, degree, name) => (name, degree)
}.values.toDF("topic","degree").orderBy(desc("degree")).show()

// 7.8 작은 세상 네트워크
val triCountGraph = interesting.triangleCount()
triCountGraph.vertices.map(x => x.2).stats()

val maxTrisGraph = interestingDegrees.mapValues(d => d*(d-1)/2.0)
val clusterCoef = triCountGraph.vertices.
    innerJoin(maxTrisGraph) { (vertexId, triCount, maxTris) => {
        if (maxTris == 0) 0 else triCount / maxTris
    }
}

clusterCoef.map(_._2).sum() / interesting.vertices.count()

def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    def minThatExists(k: VertexId): Int = {
      math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }

def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
    : Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map { case (v, d) => v -> (d + 1) }
    if (b != mergeMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else {
      Iterator.empty
    }
  }

def iterate(e: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])] = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
    checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
}

val fraction = 0.02
val replacement = false
val sample = interesting.vertices.map(v => v._1).
    sample(replacement, fraction, 1729L)
val ids = sample.collect().toSet

val mapGraph = graph.mapVertices((id, v) => {
    if (ids.contains(id)) {
        Map(id -> 0)
    } else {
    Map[VertexId, Int]()
    }
})

val start = Map[VertexId, Int]()
val res = mapGraph.pregel(start)(update,iterate,mergeMaps)

val paths = res.vertices.flatMap { case (id, m) =>
    m.map { case (k,v) =>
        if (id < k) {
            (id, k, v)
        } else {
            (k, id, v)
        }
    }
}.distinct()
paths.cache()
paths.map(_._3).filter(_ > 0).stats()

val hist = paths.map(_._3).countByValue()
hist.toSeq.sorted.foreach(println)