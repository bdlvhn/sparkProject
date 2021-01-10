// 뉴욕 택시 운행 데이터로 위치 및 시간 데이터 분석하기

// 8.3 지리 데이터와 Esri Geometry API, 그리고 Spray
import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}
import RichGeometry._
import spray.json._

class RichGeometry(val geometry: Geometry,
    val spatialReference: SpatialReference = SpatialReference.create(4326)) {

    def area2D(): Double = geometry.calculateArea2D()

    def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, spatialReference)
    }

    def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, spatialReference)
    }
}

object RichGeometry {
  implicit def wrapRichGeo(g: Geometry) = {
    new RichGeometry(g)
  }
}

case class Feature(id: Option[JsValue],
                   properties: Map[String, JsValue],
                   geometry: RichGeometry) {
  def apply(property: String): JsValue = properties(property)
  def get(property: String): Option[JsValue] = properties.get(property)
}

case class FeatureCollection(features: Array[Feature])
    extends IndexedSeq[Feature] {
  def apply(index: Int): Feature = features(index)
  def length: Int = features.length
}

implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
    def write(f: Feature): JsObject = {
      val buf = scala.collection.mutable.ArrayBuffer(
        "type" -> JsString("Feature"),
        "properties" -> JsObject(f.properties),
        "geometry" -> f.geometry.toJson)
      f.id.foreach(v => { buf += "id" -> v})
      JsObject(buf.toMap)
    }

    def read(value: JsValue): Feature = {
      val jso = value.asJsObject
      val id = jso.fields.get("id")
      val properties = jso.fields("properties").asJsObject.fields
      val geometry = jso.fields("geometry").convertTo[RichGeometry]
      Feature(id, properties, geometry)
    }
}

// 8.4 뉴욕 택시 운행 데이터 준비하기
val taxiRaw = spark.read.option("header","true").csv("/home/hoon/project/sparkAdv/ch08_geotime/data")
taxiRaw.show()

case class Trip(
    license: String,
    pickupTime: Long,
    dropoffTime: Long,
    pickUpX: Double,
    pickUpY: Double,
    dropoffX: Double,
    dropoffY: Double)

class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
}

def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
        "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
}

def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
}

def parse(line: Row): Trip = {
    val rr = new RichRow(line)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
    )
  }
}

// 8.4.1 잘못된 레코드 대규모로 다루기
def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
}

val safeParse = safe(parse)
val taxiParsed = taxiRaw.rdd.map(safeParse)
taxiParsed.map(_.isLeft).
    countByValue().
    foreach(println)

val taxiGood = taxiParsed.map(_.left.get).toDS
taxiGood.cache()

val hours = (pickup: Long, dropoff: Long) => {
  TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
}
val hoursUDF = udf(hours)

import org.apache.spark.sql.functions._

taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
    count().sort("h").
    show()

taxiGood.where(hoursUDF($"pickupTime",$"dropoffTime") < 0).
    collect().
    foreach(println)

spark.udf.register("hours", hours)
val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

// 8.4.2 지리 정보 분석하기
import com.cloudera.datascience.geotime._
import com.esri.core.geometry.Point
import GeoJsonProtocol._
import spary.json._

val geojson = scala.io.Source.
    fromFile("/home/hoon/project/sparkAdv/ch08_geotime/data/nyc-boroughs.geojson").mkString

val features = geojson.parseJson.convertTo[FeatureCollection]

val p = new Point(-73.994499, 40.75066)
val borough = features.find(f => f.geometry.contains(p))

val areaSortedFeatures = features.sortBy { f => 
  val borough = f("boroughCode").convertTo[Int]
  (borough, -f.geometry.area2D())
}

val bFeatures = sc.broadcast(areaSortedFeatures)

val bLookup = (x: Double, y: Double) => {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(new Point(x, y))
  })
  feature.map(f => {
    f("borough").convertTo[String]
  }).getOrElse("NA")
}
val boroughUDF = udf(bLookup)

taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
taxiClean.where(boroughUDF($"dropoffX",$"dropoffY") === "NA").show()

val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")
taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

// 8.5 스파크에서 세션화 작업 수행하기
val sessions = taxiDone.
    repartition($"license").
    sortWithinPartitions($"license", $"pickupTime").
    cache()
sessions.cache()

def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

val boroughDurations: DataFrame =
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds")

boroughDurations.
    selectExpr("floor(seconds/3600) as hours").
    groupBy("hours").
    count().
    sort("hours").
    show()

boroughDurations.
  where("seconds > 0").
  groupBy("borough").
  agg(avg("seconds"), stddev("seconds")).
  show()