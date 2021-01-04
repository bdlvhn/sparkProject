// Ch06, Latent Semantic Analysis

// 6.1 문서-단어 행렬
// TF-IDF (scala에서 별도로 제공)
def termDocWeight(termFrequencyIndoc:Int, totalTermsIndoc:Int,
    termFreqInCorpus:Int, totalDocs:Int): Double = {
    val tf = termFrequencyIndoc.toDouble / totalTermsIndoc // TF : 단어출현 빈도 / 문서 내 전체 단어
    val docFreq = totalDocs.toDouble / termFreqInCorpus // 뭉치의 전체 문서 / 단어출현 빈도
    val idf = math.log(docFreq) // IDF : 역문서빈도
    tf * idf
}

/*
> TF - IDF에 대한 두 가지 직관
1. 특정 문서에 어떤 단어가 많이 나올수록 중요한 단어
2. 전역적으로 볼때 모든 단어의 가치가 똑같지는 않다 
> 전제
단어에 집중하고 꾸밈말은 무시
각각의 단어는 한 가지의 의미만 가지며, 다의어는 다루지 않음
*/

// 6.3 파싱하여 데이터 준비하기

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

val path = "/home/hoon/project/sparkAdv/ch06_lsa/data/wikidump.xml"
@transient val conf = new Configuration()
conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
val kvs = spark.sparkContext.newAPIHadoopFile(path,classOf[XMLInputFormat],classOf[LongWritable],classOf[Text],conf)
val rawXmls = kvs.map(_._2.toString).toDS()

import edu.umd.cloud9.collection.wikipedia.language._
import edu.umd.cloud9.collection.wikipedia._

 def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
    val page = new EnglishWikipediaPage()
    val hackedPageXml = pageXml.replaceFirst(
      "<text bytes=\"\\d+\" xml:space=\"preserve\">", "<text xml:space=\"preserve\">")

    WikipediaPage.readPage(page, hackedPageXml)
    if (page.isEmpty || !page.isArticle || page.isRedirect || page.getTitle.contains("(disambiguation)")) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
  }

val docTexts = rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)

// 6.4 표제어 추출
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties
import org.apache.spark.sql.Dataset

def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
}

def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
}

def plainTextToLemmas(text: String, stopWords: Set[String],
    pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)

    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences.asScala;
        token <- sentence.get(classOf[TokensAnnotation]).asScala) {
    val lemma = token.get(classOf[LemmaAnnotation])
    if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
    }
}
    lemmas
}

val stopWords = scala.io.Source.fromFile("/home/hoon/project/sparkAdv/ch06_lsa/data/stopwords.txt").getLines().toSet
val bStopWords = spark.sparkContext.broadcast(stopWords)

val terms: Dataset[(String,Seq[String])] = 
    docTexts.mapPartitions { iter =>
        val pipeline = createNLPPipeline()
        iter.map { case(title, contents) =>
        (title, plainTextToLemmas(contents, bStopWords.value, pipeline))
    }
}

// 6.5 단어빈도-역문서빈도(TF-IDF) 계산하기

val termsDF = terms.toDF("title","terms")
val filtered = termsDF.where(size($"terms") > 1)

import org.apache.spark.ml.feature.CountVectorizer

val numTerms = 20000
val countVectorizer = new CountVectorizer().
    setInputCol("terms").setOutputCol("termFreqs").
    setVocabSize(numTerms)
val vocabModel = countVectorizer.fit(filtered)
val docTermFreqs = vocabModel.transform(filtered)
docTermFreqs.cache()

import org.apache.spark.ml.feature.IDF

val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidVec")
val idfModel = idf.fit(docTermFreqs)
val docTermMatrix = idfModel.transform(docTermFreqs).select("title","tfidVec")

val termIds: Array[String] = vocabModel.vocabulary
val docIds = docTermFreqs.rdd.map(_.getString(0)).
    zipWithUniqueId().
    map(_.swap).
    collect().toMap

import org.apache.spark.mllib.linalg.{Vectors,
    Vector => MLLibVector}
import org.apache.spark.ml.linalg.{Vector => MLVector}

val vecRdd = docTermMatrix.select("tfidVec").rdd.map { row =>
    Vectors.fromML(row.getAs[MLVector]("tfidVec"))
}

import org.apache.spark.mllib.linalg.distributed.RowMatrix

vecRdd.cache()
val mat = new RowMatrix(vecRdd)
val k = 1000
val svd = mat.computeSVD(k,computeU=true)

// 6.7 중요한 의미 찾기
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

def topTermsInTopConcepts(
    svd: SingularValueDecomposition[RowMatrix,Matrix],
    numConcepts: Int,
    numTerms: Int, termIds: Array[String]) : Seq[Seq[(String,Double)]] = {

    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String,Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs+v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
            case (score, id) => (termIds(id), score)
        }
    }
    topTerms
}

def topDocsInTopConcepts(
    svd: SingularValueDecomposition[RowMatrix,Matrix],
    numConcepts: Int, numDocs: Int, docIds: Map[Long,String]) : Seq[Seq[(String,Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String,Double)]]()
    for (i <- 0 until numConcepts) {
        val docWeights = u.rows.map(_.toArray(i))zipWithUniqueId()
        topDocs += docWeights.top(numDocs).map {
            case (score, id) => (docIds(id), score)
        }
    }
    topDocs
}

// 6.9 단어와 단어 사이의 연관도

import breeze.linalg.{DenseMatrix => BDenseMatrix}
import com.cloudera.datascience.lsa.LSAQueryEngine

val termIdfs = idfModel.idf.toArray
val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)

queryEngine.printTopTermsForTerm("algorithm")
queryEngine.printTopTermsForTerm("minkowski")

// 6.10 문서와 문서 사이의 연관도
def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }


def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
val normalizedUS: RowMatrix = distributedRowsNormalized(US)

import org.apache.spark.mllib.linalg.Matrices

def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).
    lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
    val docScores = normalizedUS.multiply(docRowVec)
    val allDocWeights = docScores.rows.map(_.toArray(0)).
        zipWithUniqueId()

    allDocWeights.filter(!_._1.isNaN).top(10)
}


val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
val idDocs: Map[String, Long] = docIds.map(_.swap)

def printTopDocsForDoc(doc: String): Unit = {
    val idWeights = topDocsForDoc(idDocs(doc))
    println(idWeights.map { case (score, id) =>
    (docIds(id), score)
    }.mkString(", "))
}

queryEngine.printTopTermsForTerm("minkowski")

// 6.11 문서와 단어 사이의 연관도 (incomplete)

def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until svd.V.numCols).
    map(i => svd.V(termId, i)).toArray
    val rowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    val docScores = US.multiply(rowVec)

    val allDocWeights = docScores.rows.map(_.toArray(0)).
    zipWithUniqueId()
    allDocWeights.top(10)
}

def printTopDocsForTerm(term: String): Unit = {
    val idWeights = topDocsForTerm(US, svd.V, idTerms(term))
    println(idWeights.map { case (score, id) =>
    (docIds(id), score)
    }.mkString(", "))
}

queryEngine.printTopTermsForTerm("minkowski")


// 6.12 여러 개의 단어로 질의하기(incomplete)

termIdfs = idfModel.idf.toArray

