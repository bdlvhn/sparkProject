// 몬테카를로 시뮬레이션으로 금융 리스크 추정하기
// 9.5 전처리하기

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io.file

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
Localdate.parse("2014-10-24")

def readYahooHistory(file: File): Array[(LocalDate, Double)] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val lines = scala.io.Source.fromFile(file).getLines().toSeq
    lines.tail.flatMap { line =>
      Try {
        val cols = line.split(',')
        val date = LocalDate.parse(cols(0), formatter)
        val value = cols(4).toDouble
        (date, value)
      }.toOption
    }.toArray
  }

val start = LocalDate.of(2009, 10, 23)
val end = LocalDate.of(2014, 10, 23)

val stocksDir = new File("stocks/")
val files = stocksDir.listFiles()
val allStocks = files.iterator.flatMap { file =>
  try {
    Some(readYahooHistory(file))
  } catch {
    case e: Exception => None
  }
}
val rawStocks = allStocks.filter(_.size >= 260 * 5 + 10)

val factorsPrefix = "factors/"
val rawFactors = Array("^GSPC.csv", "^IXIC.csv", "^TYX.csv", "^FVX.csv").
  map(x => new File(factorsPrefix + x)).
  map(readYahooHistory)

def trimToRegion(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
    : Array[(LocalDate, Double)] = {
    var trimmed = history.dropWhile(_._1.isBefore(start)).
      takeWhile(x => x._1.isBefore(end) || x._1.isEqual(end))
    if (trimmed.head._1 != start) {
      trimmed = Array((start, trimmed.head._2)) ++ trimmed
    }
    if (trimmed.last._1 != end) {
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    }
    trimmed
  }

def fillInHistory(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate)
    : Array[(LocalDate, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(LocalDate, Double)]()
    var curDate = start
    while (curDate.isBefore(end)) {
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }

      filled += ((curDate, cur.head._2))

      curDate = curDate.plusDays(1)
      // Skip weekends
      if (curDate.getDayOfWeek.getValue > 5) {
        curDate = curDate.plusDays(2)
      }
    }
    filled.toArray
  }

val stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

val factors = rawFactors.
    map(trimToRegion(_, start, end)).
    map(fillInHistory(_, start, end))

(stocks ++ factors).forall(_.size == stocks(0).size)

// 9.6 요인 가중치 결정하기
def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double] = {
    history.sliding(10).map { window =>
      val next = window.last._2
      val prev = window.head._2
      (next - prev) / prev
    }.toArray
}

val stocksReturns = stocks.map(twoWeekReturns).toArray.toSeq
val factorsReturns = factors.map(twoWeekReturns)

def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- histories.head.indices) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
}

val factorMat = factorMatrix(factorsReturns)

def featurize(factorReturns: Array[Double]): Array[Double] = {
    val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
    val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    squaredReturns ++ squareRootedReturns ++ factorReturns
}

val factorFeatures = factorMat.map(featurize)

def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])
    : OLSMultipleLinearRegression = {
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(instrument, factorMatrix)
    regression
}

val factorWeights = stocksReturns.
    map(linearModel(_, factorFeatures)).
    map(_.estimateRegressionParameters()).
    toArray

// 9.7 표본추출
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.util.StatCounter
import breeze.plot._

def plotDistribution(samples: Array[Double]): Figure = {
    val min = samples.min
    val max = samples.max
    val stddev = new StatCounter(samples).stdev
    val bandwidth = 1.06 * stddev * math.pow(samples.size, -.2)

    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val kd = new KernelDensity().
      setSample(samples.toSeq.toDS.rdd).
      setBandwidth(bandwidth)
    val densities = kd.estimate(domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
}

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.distributioni.MultivariateNormalDistribution

val factorCor = new PearsonsCorrelation(factorMat).getCorrelationMatrix().getData()
println(factorCor.map(_.mkString("\t")).mkString("\n"))

val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray

val factorsDist = new MultivariateNormalDistribution(factorMeans,factorCov)

factorsDist.sample()
factorsDist.sample()

// 9.8 실험 실행하기

val parallelism = 1000
val baseSeed = 1496

val seeds = (baseSeed until baseSeed + parallelism)
val seedDS = seeds.toDS().repartition(parallelism)

def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
    var instrumentTrialReturn = instrument(0)
    var i = 0
    while (i < trial.length) {
      instrumentTrialReturn += trial(i) * instrument(i+1)
      i += 1
    }
    instrumentTrialReturn
}

def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
    var totalReturn = 0.0
    for (instrument <- instruments) {
      totalReturn += instrumentTrialReturn(instrument, trial)
    }
    totalReturn / instruments.size
}

import org.apache.commons.math3.random.MersenneTwister

def trialReturns(
      seed: Long,
      numTrials: Int,
      instruments: Seq[Array[Double]],
      factorMeans: Array[Double],
      factorCovariances: Array[Array[Double]]): Seq[Double] = {
    val rand = new MersenneTwister(seed)
    val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
      factorCovariances)

    val trialReturns = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val trialFactorReturns = multivariateNormal.sample()
      val trialFeatures = featurize(trialFactorReturns)
      trialReturns(i) = trialReturn(trialFeatures, instruments)
    }
    trialReturns
}

val numTrials = 10000000
val trials = seedDS.flatMap(
    trialReturns(_, numTrials / parallelism,
    factorWeights, factorMeans, factorCov))
trials.cache()

def fivePercentVaR(trials: Dataset[Double]): Double = {
    val quantiles = trials.stat.approxQuantile("value", Array(0.05), 0.0)
    quantiles.head
}
val valueAtRisk = fivePercentVaR(trials)

def fivePercentCVaR(trials: Dataset[Double]): Double = {
    val topLosses = trials.orderBy("value").limit(math.max(trials.count().toInt / 20, 1))
    topLosses.agg("value" -> "avg").first()(0).asInstanceOf[Double]
}
val conditionalValueAtRisk = fivePercentCVaR(trials)

// 9.9 수익 분포 시각화하기
import org.apache.spark.sql.functions

def plotDistribution(samples: Array[Double]): Figure = {
    val min = samples.min
    val max = samples.max
    val stddev = new StatCounter(samples).stdev
    val bandwidth = 1.06 * stddev * math.pow(samples.size, -.2)

    // Using toList before toArray avoids a Scala bug
    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val kd = new KernelDensity().
      setSample(samples.toSeq.toDS.rdd).
      setBandwidth(bandwidth)
    val densities = kd.estimate(domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
    f
}
plotDistribution(trials)

def bootstrappedConfidenceInterval(
      trials: Dataset[Double],
      computeStatistic: Dataset[Double] => Double,
      numResamples: Int,
      probability: Double): (Double, Double) = {
    val stats = (0 until numResamples).map { i =>
      val resample = trials.sample(true, 1.0)
      computeStatistic(resample)
    }.sorted
    val lowerIndex = (numResamples * probability / 2 - 1).toInt
    val upperIndex = math.ceil(numResamples * (1 - probability / 2)).toInt
    (stats(lowerIndex), stats(upperIndex))
}
bootstrappedConfidenceInterval(trials, fivePercentVaR, 100, .05)
bootstrappedConfidenceInterval(trials, fivePercentCVaR, 100, .05)

def kupiecTestPValue(
      stocksReturns: Seq[Array[Double]],
      valueAtRisk: Double,
      confidenceLevel: Double): Double = {
    val failures = countFailures(stocksReturns, valueAtRisk)
    val total = stocksReturns.head.length
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }

import org.apache.commons.math3.distribution.ChiSquaredDistribution

def kupiecTestPValue(
      stocksReturns: Seq[Array[Double]],
      valueAtRisk: Double,
      confidenceLevel: Double): Double = {
    val failures = countFailures(stocksReturns, valueAtRisk)
    val total = stocksReturns.head.length
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }