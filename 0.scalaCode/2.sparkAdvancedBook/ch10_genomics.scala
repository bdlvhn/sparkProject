// BDG 프로젝트와 유전체학 데이터 분석하기

// 10.2 ADAM CLI를 이용한 유전체학 데이터 정리
import org.bdgenomics.adam.rdd.ADAMContext._

val readsRDD = sc.loadAlignments("/path/to/file/HG00103")
readsRDD.rdd.first()
readsRDD.rdd.count()

val uniq_chr = (readsRDD.rdd.
    map(_.getContigName).
    distinct().
    collect())
uniq_chr.sorted.foreach(println)

val cftr_reads = (readsRDD.rdd.
    filter(_.getContigName == "7")
    filter(_.getStart <= 117149189)
    filter(_.getEnd > 117149189).
    collect())
cftr_reads.length

import org.apache.parquet.filter2.dsl.Dsl._

val chr = BinaryColumn("contigName")
val start = LongColumn("start")
val end = LongColumn("end")

val cftrLocusPredicate = (
    chr === "7" && start <= 117149189 && end >= 117149189)

val readsRDD = sc.loadParquetAlignments(
    "/path/to/file/HG00103",Some(cftrLocusPredicate))

// 1.03 ENCODE 데이터로부터 전사인자 결합 부위 예측하기
import org.bdgenomics.adam.rdd.ADAMContext._
(sc.
    loadBed("/path01").
    saveAsParquet("path02"))

val cellLines = Vector("GM12878", "K562", "BJ", "HEK293", "H54", "HepG2")
val dataByCellLine = cellLines.map(cellLine => {
      val dnaseRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, s"dnase/$cellLine.DNase.narrowPeak").toString).rdd
        .map(f => ReferenceRegion.unstranded(f)).map(r => (r, r)))

val hdfsPrefix = "/user/ds/genomics"
val localPrefix = "/user/ds/genomics"

val bHg19Data = sc.broadcast(
      new TwoBitFile(
        new LocalFileByteAccess(
          new File(Paths.get(localPrefix, "hg19.2bit").toString))))

    def distanceToClosest(loci: Vector[Long], query: Long): Long = {
      loci.map(x => math.abs(x - query)).min
}

 val bPwmData = sc.broadcast(Vector(
      Map('A'->0.4553,'C'->0.0459,'G'->0.1455,'T'->0.3533),
      Map('A'->0.1737,'C'->0.0248,'G'->0.7592,'T'->0.0423),
      Map('A'->0.0001,'C'->0.9407,'G'->0.0001,'T'->0.0591),
      Map('A'->0.0051,'C'->0.0001,'G'->0.9879,'T'->0.0069),
      Map('A'->0.0624,'C'->0.9322,'G'->0.0009,'T'->0.0046),
      Map('A'->0.0046,'C'->0.9952,'G'->0.0001,'T'->0.0001),
      Map('A'->0.5075,'C'->0.4533,'G'->0.0181,'T'->0.0211),
      Map('A'->0.0079,'C'->0.6407,'G'->0.0001,'T'->0.3513),
      Map('A'->0.0001,'C'->0.9995,'G'->0.0002,'T'->0.0001),
      Map('A'->0.0027,'C'->0.0035,'G'->0.0017,'T'->0.9921),
      Map('A'->0.7635,'C'->0.0210,'G'->0.1175,'T'->0.0980),
      Map('A'->0.0074,'C'->0.1314,'G'->0.7990,'T'->0.0622),
      Map('A'->0.0138,'C'->0.3879,'G'->0.0001,'T'->0.5981),
      Map('A'->0.0003,'C'->0.0001,'G'->0.9853,'T'->0.0142),
      Map('A'->0.0399,'C'->0.0113,'G'->0.7312,'T'->0.2177),
      Map('A'->0.1520,'C'->0.2820,'G'->0.0082,'T'->0.5578),
      Map('A'->0.3644,'C'->0.3105,'G'->0.2125,'T'->0.1127)))

  def scorePWM(ref: String): Double = {
      val score1 = (ref.sliding(bPwmData.value.length)
        .map(s => {
          s.zipWithIndex.map(p => bPwmData.value(p._2)(p._1)).product})
        .max)
      val rc = Alphabet.dna.reverseComplementExact(ref)
      val score2 = (rc.sliding(bPwmData.value.length)
        .map(s => {
          s.zipWithIndex.map(p => bPwmData.value(p._2)(p._1)).product})
        .max)
      math.max(score1, score2)
}

val tssRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, "gencode.v18.annotation.gtf").toString).rdd
      .filter(_.getFeatureType == "transcript")
      .map(f => (f.getContigName, f.getStart)))

 val bTssData = sc.broadcast(tssRDD
      .groupBy(_._1)
      .map(p => (p._1, p._2.map(_._2.toLong).toVector))
      .collect().toMap)

 val phylopRDD = (sc.loadParquetFeatures(Paths.get(hdfsPrefix, "phylop").toString).rdd
      .filter(f => f.getStart <= f.getEnd)
      .map(f => (ReferenceRegion.unstranded(f), f)))

 val dnaseRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, s"dnase/$cellLine.DNase.narrowPeak").toString).rdd
        .map(f => ReferenceRegion.unstranded(f)).map(r => (r, r)))

val chipseqRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, s"chip-seq/$cellLine.ChIP-seq.CTCF.narrowPeak").toString).rdd
    .map(f => ReferenceRegion.unstranded(f)).map(r => (r, r)))

val dnaseWithLabelRDD = (LeftOuterShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)

val dnaseWithLabelRDD = (LeftOuterShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
    .partitionAndJoin(dnaseRDD, chipseqRDD)
    .map(p => (p._1, p._2.size))
    .reduceByKey(_ + _)
    .map(p => (p._1, p._2 > 0))
    .map(p => (p._1, p)))

def aggPhylop(values: Vector[Double]) = {
        val avg = values.sum / values.length
        val m = values.min
        val M = values.max
        (avg, m, M)
}


val dnaseWithPhylopRDD = (LeftOuterShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
    .partitionAndJoin(dnaseRDD, phylopRDD)
    .filter(!_._2.isEmpty)
    .map(p => (p._1, p._2.get.getScore.doubleValue))
    .groupByKey()
    .map(p => (p._1, aggPhylop(p._2.toVector))))

val examplesRDD = (InnerShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
        .partitionAndJoin(dnaseWithLabelRDD, dnaseWithPhylopRDD)
        .map(tup => (tup._1, tup._2, bHg19Data.value.extract(tup._1._1)))
        .filter(!_._3.contains("N"))
        .map(tup => {
          val region = tup._1._1
          val label = tup._1._2
          val contig = region.referenceName
          val start = region.start
          val end = region.end
          val phylopAvg = tup._2._1
          val phylopMin = tup._2._2
          val phylopMax = tup._2._3
          val seq = tup._3
          val pwmScore = scorePWM(seq)
          val closestTss = math.min(
            distanceToClosest(bTssData.value(contig), start),
            distanceToClosest(bTssData.value(contig), end))
          val tf = "CTCF"
          (contig, start, end, pwmScore, phylopAvg, phylopMin, phylopMax, closestTss, tf, cellLine, label)}))
      examplesRDD
})

val preTrainingData = dataByCellLine.reduce(_ ++ _)
    preTrainingData.cache()
    preTrainingData.count() // 802059
preTrainingData.filter(_._11 == true).count() // 220344