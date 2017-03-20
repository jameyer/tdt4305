// register function we will use to convert the price column from string to int
spark.udf.register("myConvertCurrency", (input: String) => java.text.NumberFormat.getCurrencyInstance(java.util.Locale.US).parse(input).intValue.toInt);

val listingsRaw = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_us.csv");
val listings = listingsRaw.withColumn("price", callUDF("myConvertCurrency", listingsRaw("price")));
val calendar = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/calendar_us.csv");

listings.createOrReplaceTempView("listings");
calendar.createOrReplaceTempView("calendar");

// average number of listings per host

// create view for number of listings per host
val listingsPerHost = spark.sql("select count(*) as count from listings group by host_id");

listingsPerHost.createOrReplaceTempView("listingsPerHost");

// get average number of listings per host
val averageNumberOfListingsPerHost = spark.sql("select avg(count) from listingsPerHost").head.getDouble(0);

println("Average number of listings per host: " + averageNumberOfListingsPerHost);

// percentage of hosts with more than 1 listing
// note that listingsPerHost.count is equal to the number of unique hosts
val percentageOfHosts = spark.sql("select count from listingsPerHost where count > 1").count.toFloat / listingsPerHost.count;

println("Percentage of hosts with more than 1 listing: " + percentageOfHosts);

// for each city, find top 3 hosts with the highest income
val cities = spark.sql("select distinct city from listings").map(_.getString(0)).collect;

for (city <- cities) {
	println("--- " + city + " ---");
	
	// create view for amount of nights a listing was booked
	val countsPerListings = spark.sql("select count(*) as count, listing_id from calendar JOIN listings ON listings.id = calendar.listing_id where available = 't' and city = '" + city + "'" + "group by listing_id");

	countsPerListings.createOrReplaceTempView("countsPerListings");

	// find top 3 hosts
	val topHosts = spark.sql("select first(host_name), sum(price * countsPerListings.count) as income from listings join countsPerListings on listings.id = countsPerListings.listing_id group by host_id order by income desc limit 3").map(x => x.getString(0) + " ($" + x.getLong(1) + ")").collect;

	println("Top 3 hosts with highest income are " + topHosts.mkString(", "));
}

sys.exit