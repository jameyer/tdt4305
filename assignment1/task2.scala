val listings = listingsRaw.withColumn("price", callUDF("myConvertCurrency", listingsRaw("price")));

listings.createOrReplaceTempView("listings");

// 2b

println("Distinct values for each column in listings");

listings.columns.foreach(column => println(column + ": " + spark.sql("select count(distinct " + column + ") from listings").head.getLong(0)))

// 2c
val cities = spark.sql("select distinct city from listings").map(_.getString(0)).collect;

println("Listings from " + cities.length + " cities - " + cities.mkString(", ") + " are contained in the dataset");

sys.exit