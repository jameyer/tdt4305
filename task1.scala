import scala.collection.mutable.WrappedArray;
import scala.collection.mutable.ListBuffer;

// replaces special characters, and places the description in an array with each word
spark.udf.register( "myNormalizeText", (input: String) => input.replaceAll(",", " ").split(" ").map(s => s.replaceAll("[^a-zA-Z0-9 -]", "").toLowerCase.trim) );

// read listings
val listings = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_us.csv").filter($"description".isNotNull).withColumn("description", callUDF("myNormalizeText", $"description"));
val totalNumberOfDocuments = listings.count;

// read neighbourhoods
spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv("C:/Users/jon/airbnb_datasets/listings_ids_with_neighborhoods.tsv").filter($"neighbourhood".isNotNull).createOrReplaceTempView("neighbourhoods");

listings.createOrReplaceTempView("listings")

var words: Seq[String] = null;

if (args(1).equals("-l")) {
	val listing_id = args(2);
	val description = spark.sql("select description from listings where id = '" + listing_id + "'");
	
	words = description.first.getAs[WrappedArray[String]](0);
} else if (args(1).equals("-n")) {
	val neighbourhood = args(2);
	val description = spark.sql("select collect_list(description) from listings join neighbourhoods on listings.id = neighbourhoods.id where neighbourhood = '" + neighbourhood + "'");
	
	words = description.first.getAs[WrappedArray[WrappedArray[String]]](0).flatten;
}

val wordsDistinct = words.distinct;
val listBuffer = new ListBuffer[(String, Double)];
val filteredWords = wordsDistinct.filter(_.length > 0);

for (word <- filteredWords) {
	val occurencesInCurrentListing = words.count(_.equals(word));
	val numberOfDocumentsWithWord = spark.sql("select count(*) from listings where array_contains(description, '" + word + "')").cache.collect.head.getLong(0);
		
	val tf = occurencesInCurrentListing.toFloat / wordsDistinct.length;
	val idf = totalNumberOfDocuments.toFloat / numberOfDocumentsWithWord;
	val tf_idf = tf * idf;
		
	listBuffer += ((word, tf_idf));
		
	// println("---------------------------------------------------")
	// println("occurencesInCurrentListing: " + occurencesInCurrentListing);
	// println("numberOfDocumentsWithWord: " + numberOfDocumentsWithWord);
	// println("idf: " + idf);
	// println(word + ": " + tf_idf);
	// println("---------------------------------------------------")
}

val tfidfs = listBuffer.toList.sortBy(_._2).reverse.take(100);

tfidfs.foreach(x => println(x._1 + ": " + x._2));