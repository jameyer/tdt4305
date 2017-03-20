import scala.collection.mutable.WrappedArray

val listings = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_us.csv");
val neighbourhoods = spark.read.json("C:/Users/jon/airbnb_datasets/neighbourhoods.geojson");

listings.createOrReplaceTempView("listings");
neighbourhoods.createOrReplaceTempView("neighbourhoods");

// https://github.com/smagnacco/scala-point-in-polygon/blob/master/src/main/scala/org/opensource/gis/polygon/PolygonUtils.scala
import scala.annotation.tailrec

/**
 * Source: http://alienryderflex.com/polygon/
 *
 * Third algorithm with precalc optimization
 */

case class GeoPoint(latitude: Double, longitude: Double)

case class Polygon(points: List[GeoPoint], neighbourhood: String) {
  def corners = points.size
  def horizontalCoordinates = points map (_.latitude)
  def verticalCoordinates = points map (_.longitude)
}

object PolygonUtils {
  def pointInPolygon(point: GeoPoint, polygon: Polygon): Boolean = {

      @tailrec
      def precalc(polyCorners: Int, i: Int, j: Int, polyX: Array[Double], polyY: Array[Double],
                  constant: List[Double], multiple: List[Double]): (List[Double], List[Double])= {
        i match {
          case i if i == polyCorners => (constant, multiple)
          case i if polyY{j} == polyY{i} => precalc(polyCorners, i + 1, i, polyX, polyY, polyX{i} :: constant, 0d :: multiple)
          case i :Int => {
            val k = polyX{i} - (polyY{i} * polyX{j}) / (polyY{j} - polyY{i}) + (polyY{i} * polyX{i} ) / (polyY{j} - polyY{i})
            val m = (polyX{j} - polyX{i}) / (polyY{j} - polyY{i})
            precalc(polyCorners, i + 1, i, polyX, polyY, k :: constant, m :: multiple)
          }
        }
      }

      @tailrec
      def isInside(point: GeoPoint, polyCorners: Int, i: Int, j: Int, polyX: Array[Double], polyY: Array[Double],
                    constant: Array[Double], multiple: Array[Double], oddNodes: Boolean): Boolean = {
        val x = point.latitude
        val y = point.longitude
        i match {
          case i if i == polyCorners => oddNodes
          case i if polyY{i} < y && polyY{j} >= y || polyY{j} < y && polyY{i}>=y => {
            val odd = oddNodes ^ ( y * multiple{i} + constant{i} < x)
            isInside(point, polyCorners, i + 1, i, polyX, polyY, constant, multiple, odd)
          }
          case i: Int => isInside(point, polyCorners, i + 1, i, polyX, polyY, constant, multiple, oddNodes)
        }
      }

    val polyX: Array[Double] = polygon.horizontalCoordinates.toArray
    val polyY: Array[Double] = polygon.verticalCoordinates.toArray

    val tuple = precalc(polygon.corners, 0, polygon.corners - 1, polyX, polyY, List(), List())

    val oddNodes = false
    isInside(point, polygon.corners, 0, polygon.corners - 1, polyX, polyY, tuple._1.toArray, tuple._2.toArray, oddNodes)
  }
}

val neighbourhoodWithCoordinates = neighbourhoods.withColumn("neighbourhood", explode($"features.properties.neighbourhood")).select($"neighbourhood", $"features.geometry")

