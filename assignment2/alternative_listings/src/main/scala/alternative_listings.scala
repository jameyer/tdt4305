/*

Scafolding of Scala/Spark app for the Big Data assignment.
To create a jar file, install SBT (http://www.scala-sbt.org/),
organize your project into the following structure:

./tf_idf.sbt
./src
./src/main
./src/main/scala
./src/main/scala/tf_idf.scala

and in the root folder of the project, run: 'sbt package'.
This should create a file in ./target/scala-2.XX/tf-idf_2.XX-1.0.jar.

Try to run the file with command: spark-submit tf-idf_2.XX-1.0.jar <arguments>.

For more info, visit: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.

*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.udf
import math._

object AlternativeListings {
	def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
		val R = 6372.8  // radius in km
		val dLat = (lat2 - lat1).toRadians
		val dLon = (lon2 - lon1).toRadians
 
		val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
		val c = 2 * asin(sqrt(a))
		R * c
	}

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AlternativeListings")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
	
	val spark = SparkSession.builder.getOrCreate()
	import spark.implicits._

    println("AlternativeListings Assignment")
    // print arguments
    println("Printing arguments: ")
    args.foreach(println(_))
	
	// replaces special characters, and places the description in an array with each word
	val normalizeTextUdf = udf( (input: String) => input.split(",").map(s => s.replaceAll("[^a-zA-Z0-9 -]", "").toLowerCase.trim) )
	val convertCurrencyUdf = udf ( (input: String) => java.text.NumberFormat.getCurrencyInstance(java.util.Locale.US).parse(input).intValue );

	val path = "./"

	// read listings
	spark.read.format("csv")
		.option("header", "true")
		.option("delimiter", "\t")
		.csv(path + "listings_us.csv")
		.withColumn("amenities", normalizeTextUdf($"amenities")).withColumn("price", convertCurrencyUdf($"price")).createOrReplaceTempView("listings")
			
	// read calendar
	spark.read.format("csv")
		.option("header", "true")
		.option("delimiter", "\t")
		.csv(path + "calendar_us.csv")
		.createOrReplaceTempView("calendar")
		
	val currentListingId = args(0).toInt
	val currentDate = args(1)
	val pricePercent = args(2).toInt
	val maxDistance = args(3).toInt
	val limit = args(4).toInt // aka top n
	val currentListing = spark.sql("select id, name, room_type, amenities, latitude, longitude, price from listings where id = '" + currentListingId + "'");
	val currentName = currentListing.head.getString(1);
	val currentRoomType = currentListing.head.getString(2);
	val currentAmenities = currentListing.head.getAs[Seq[String]](3);
	val currentLatitude = currentListing.head.getString(4).toDouble;
	val currentLongitude = currentListing.head.getString(5).toDouble;
	val currentMaxPrice = currentListing.head.getInt(6) * (1 + (pricePercent / 100));

	// get relevant listings not accounting for distance and create a column for distance to current listing
	val distanceUdf = udf( (lat: Double, long: Double) => haversine(currentLatitude, currentLongitude, lat, long) );
	val relevantListings = spark.sql("select id, name, amenities, latitude, longitude, price from listings join calendar on listings.id = calendar.listing_id where available = 'f' and room_type = '" + currentRoomType + "' and date = '" + currentDate + "' and price < '" + currentMaxPrice + "'")
		.withColumn("distance", distanceUdf($"latitude", $"longitude"));
	relevantListings.createOrReplaceTempView("relevantListings");
		
	// update relevant listings with distance <= max
	val newRelevantListings = spark.sql("select id, name, amenities, latitude, longitude, distance, price from relevantListings where distance <= '" + maxDistance + "'");
	newRelevantListings.createOrReplaceTempView("relevantListings");
		
	// add common amenities column
	val commonAmenitiesUdf = udf ( (input: Seq[String]) => input.intersect(currentAmenities).length );
	newRelevantListings.withColumn("number_of_common_amenities", commonAmenitiesUdf($"amenities"))
		.createOrReplaceTempView("relevantListings");
	
	// get final result and limit by given top n
	val finalListings = spark.sql("select id as listing_id, name as listing_name, number_of_common_amenities, distance, price, latitude as lat, longitude as lon from relevantListings order by number_of_common_amenities desc limit " + limit);
	
	println("Results: ")
	finalListings.collect.foreach(println)
	
	println("Saving to 'alternatives.tsv'...");
	finalListings.coalesce(1).write.format("csv")
		.option("header", "true")
		.option("delimiter", "\t")
		.save("alternatives.tsv");

    println("-end-")
	
	sc.stop()
  }
}
