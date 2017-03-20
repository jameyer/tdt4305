// register function we will use to convert the price column from string to int
spark.udf.register("myConvertCurrency", (input: String) => java.text.NumberFormat.getCurrencyInstance(java.util.Locale.US).parse(input).intValue.toInt);

val listingsRaw = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_us.csv");
val listings = listingsRaw.withColumn("price", callUDF("myConvertCurrency", listingsRaw("price")));
val reviews = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/reviews_us.csv");

listings.createOrReplaceTempView("listings");
reviews.createOrReplaceTempView("reviews");

// for each city, find top 3 guests
// assuming 1 review = 1 booking here (?)
val cities = spark.sql("select distinct city from listings").map(_.getString(0)).collect;

// create view to use for top guests and most spending guest
val reviewsListings = spark.sql("select reviewer_id, reviewer_name as reviewer_name, price, city from reviews join listings on reviews.listing_id = listings.id");

reviewsListings.createOrReplaceTempView("reviewsListings");

for (city <- cities) {
	println("--- " + city + " ---");

	val topGuests = spark.sql("select reviewer_id, first(reviewer_name), (count(*) * 3) as count from reviewsListings where city = '" + city + "' group by reviewer_id order by count desc limit 3").map(x => x.getString(0) + " (" + x.getLong(1) + ")").collect;
	
	println("Top 3 guests are " + topGuests.mkString(", "));
}

// find the guest who spent the most money
val mostSpent = spark.sql("select reviewer_id, first(reviewer_name), sum(price) * 3 as spent from reviewsListings group by reviewer_id order by spent desc limit 1").map(x => x.getString(0) + " ($" + x.getLong(1) + ")").collect.head;

println("Most spending guest is " + mostSpent);

sys.exit