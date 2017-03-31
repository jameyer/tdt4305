import math._

def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
	val R = 6372.8  //radius in km
	val dLat=(lat2 - lat1).toRadians
	val dLon=(lon2 - lon1).toRadians
 
	val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
	val c = 2 * asin(sqrt(a))
	R * c
}

val normalizeTextUdf = udf( (input: String) => input.split(",").map(s => s.replaceAll("[^a-zA-Z0-9 -]", "").toLowerCase.trim) )
val convertCurrencyUdf = udf ( (input: String) => java.text.NumberFormat.getCurrencyInstance(java.util.Locale.US).parse(input).intValue );

val path = "C:/Users/jon/airbnb_datasets/"

// read listings
spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv(path + "listings_us.csv").withColumn("amenities", normalizeTextUdf($"amenities")).withColumn("price", convertCurrencyUdf($"price")).createOrReplaceTempView("listings")
		
// read calendar
spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv(path + "calendar_us.csv").createOrReplaceTempView("calendar")
	
val currentListingId = 12607303
val currentDate = "2016-12-29"
val pricePercent = 10
val maxDistance = 10
val limit = 10
val currentListing = spark.sql("select id, name, room_type, amenities, latitude, longitude, price from listings where id = '" + currentListingId + "'");
val currentName = currentListing.head.getString(1);
val currentRoomType = currentListing.head.getString(2);
val currentAmenities = currentListing.head.getAs[Seq[String]](3);
val currentLatitude = currentListing.head.getString(4).toDouble;
val currentLongitude = currentListing.head.getString(5).toDouble;
val currentMaxPrice = currentListing.head.getInt(6) * (1 + (pricePercent / 100));

// get relevant listings not accounting for distance, creating a column for distance to current listing
val distanceUdf = udf( (lat: Double, long: Double) => haversine(currentLatitude, currentLongitude, lat, long) );
val relevantListings = spark.sql("select id, name, amenities, latitude, longitude, price from listings join calendar on listings.id = calendar.listing_id where available = 'f' and room_type = '" + currentRoomType + "' and date = '" + currentDate + "' and price < '" + currentMaxPrice + "'").withColumn("distance", distanceUdf($"latitude", $"longitude"));
relevantListings.createOrReplaceTempView("relevantListings");
	
// update relevant listings with distance <= max
val relevantListings = spark.sql("select id, name, amenities, distance, price from relevantListings where distance <= '" + maxDistance + "'");
relevantListings.createOrReplaceTempView("relevantListings");
	
// add common amenities column
val commonAmenitiesUdf = udf ( (input: Seq[String]) => input.intersect(currentAmenities).length );
relevantListings.withColumn("number_of_common_amenities", commonAmenitiesUdf($"amenities")).createOrReplaceTempView("relevantListings");
	
val finalListings = spark.sql("select id as listing_id, name as listing_name, number_of_common_amenities, distance, price from relevantListings order by number_of_common_amenities desc limit ");
	
finalListings.collect.foreach(println)