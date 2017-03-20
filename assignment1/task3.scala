spark.udf.register("myConvertCurrency", (input: String) => java.text.NumberFormat.getCurrencyInstance(java.util.Locale.US).parse(input).intValue.toInt);

val listingsRaw = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_us.csv");
val reviews = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/reviews_us.csv");
val listings = listingsRaw.withColumn("price", callUDF("myConvertCurrency", listingsRaw("price")));

listings.createOrReplaceTempView("listings")

// 2b

println("Distinct values for each column in listings");

// listings.columns.foreach(column => println(column + ": " + spark.sql("select count(distinct " + column + ") from listings").head.getLong(0)))

// 2c
val cities = spark.sql("select distinct city from listings").map(_.getString(0)).collect;

println("Listings from " + cities.length + " cities - " + cities.mkString(", ") + " are contained in the dataset");

// task 3
val roomTypes = spark.sql("select distinct room_type from listings").map(_.getString(0)).collect

for (city <- cities) {
	println("--- " + city + " ---");
	
	// create subset used for this city
	val citySubset = spark.sql("select price, reviews_per_month, room_type from listings where city = '" + city + "'");
	
	citySubset.createOrReplaceTempView("citySubset");
	
	// avg price per city
	val avgPrice = spark.sql("select avg(price) from citySubset").head.getDouble(0).round;

	println("Average price: " + avgPrice);
	
	// avg price per room type per city	
	for (roomType <- roomTypes) {
		val avgPriceWithRoomType = spark.sql("select avg(price) from citySubset where room_type = '" + roomType + "'").head.getDouble(0).round;
	
		println("Average price in " + city + " for room type '" + roomType.mkString + "': " + avgPriceWithRoomType);
	}
	
	// avg number of reviews per month
	val avgNumberOfReviews = spark.sql("select avg(reviews_per_month) from citySubset").head.getDouble(0)
	
	println("Average number of reviews per month: " + avgNumberOfReviews);
	
	// for task d) and e) create a new temp view
	spark.udf.register("myNightsPerYear", (input: String) => ((if(input == null) 0.0 else input.toDouble) / 0.7) * 12);
	
	citySubset.withColumn("nights_per_year", callUDF("myNightsPerYear", citySubset("reviews_per_month"))).createOrReplaceTempView("citySubsetNightsPerYear");
	
	// estimated number of booked nights per year
	val nightsPerYear = spark.sql("select sum(nights_per_year) from citySubsetNightsPerYear").head.getDouble(0).round
	
	println("Estimated number of booked nights per year: " + nightsPerYear);
	
	// estimed money spent per year
	val moneySpentPerYear = spark.sql("select sum(nights_per_year * price) from citySubsetNightsPerYear").head.getDouble(0)
	
	println("Estimated money spent per year: " + moneySpentPerYear);
}

