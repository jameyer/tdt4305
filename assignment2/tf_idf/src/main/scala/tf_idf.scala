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

object TF_IDF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TF_IDF")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
	
	val spark = SparkSession.builder.getOrCreate()
	import spark.implicits._

    println("TF-IDF Assignment")

    // val logFile = "data.txt" // Should be some file on your system
    // val logData = sc.textFile(logFile).cache()
    // println(logData.count())
	
	val path = args(0);

    // print arguments
    println("Printing arguments: ")
    args.foreach(println(_))
	
	// replaces special characters, and places the description in an array with each word
	val normalizeTextUdf = udf( (input: String) => input.replaceAll(",", " ").split(" ").map(s => s.replaceAll("[^a-zA-Z0-9 -]", "").toLowerCase.trim) )
	
	// read listings
	val listings = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv(path + "listings_us.csv").filter($"description".isNotNull).withColumn("description", normalizeTextUdf($"description"))
	listings/*.sample(false, 0.1, 7)*/.createOrReplaceTempView("listings")
	
	var totalNumberOfDocuments: Long = 1
	var words: Seq[String] = null

	if (args(1).equals("-l")) {
		val listing_id = args(2);
		
		println("Using listing id: " + listing_id)
		
		val description = spark.sql("select description from listings where id = '" + listing_id + "'")
		
		totalNumberOfDocuments = listings.count;
		words = description.first.getAs[WrappedArray[String]](0)
	} else if (args(1).equals("-n")) {
		val neighbourhood = args(2);
		println("Using neighbourhood: " + neighbourhood)
		// read neighbourhoods
		val neighbourhoods = spark.read.format("csv").option("header", "true").option("delimiter", "\t").csv(path + "listings_ids_with_neighborhoods.tsv").filter($"neighbourhood".isNotNull)	
		neighbourhoods.createOrReplaceTempView("neighbourhoods")
		
		// for neighbourhoods each neighbourhood is the document - so we should aggregate descriptions for each neighbourhood
		spark.sql("select first(neighbourhood) as neighbourhood, collect_list(description) as description from listings join neighbourhoods on listings.id = neighbourhoods.id").createOrReplaceTempView("listings");
		
		val description = spark.sql("select description from listings where neighbourhood = '" + neighbourhood + "'")
		
		words = description.first.getAs[WrappedArray[WrappedArray[String]]](0).flatten
		
		totalNumberOfDocuments = neighbourhoods.count
		
		// need to change type of description column before it can be used in array_contains in loop below
		val flattenArrayUdf = udf ( (input: WrappedArray[String]) => input.toSeq);
		listings.withColumn("description", flattenArrayUdf($"description")).createOrReplaceTempView("listings")
	}

	val wordsDistinct = words.distinct
	val listBuffer = new ListBuffer[(String, Double)]
	val filteredWords = wordsDistinct.filter(_.length > 0)

	for (word <- filteredWords) {
		println("Checking word: " + word);
	
		val occurencesInCurrentListing = words.count(_.equals(word))
		val numberOfDocumentsWithWord = spark.sql("select count(*) from listings where array_contains(description, '" + word + "')").collect.head.getLong(0)
			
		val tf = occurencesInCurrentListing.toFloat / wordsDistinct.length
		val idf = totalNumberOfDocuments.toFloat / numberOfDocumentsWithWord
		val tf_idf = tf * idf
			
		listBuffer += ((word, tf_idf));
			
		// println("---------------------------------------------------")
		// println("occurencesInCurrentListing: " + occurencesInCurrentListing);
		// println("numberOfDocumentsWithWord: " + numberOfDocumentsWithWord);
		// println("idf: " + idf);
		// println(word + ": " + tf_idf);
		// println("---------------------------------------------------")
	}

	val tfidfs = listBuffer.toList.sortBy(_._2).reverse.take(100)

	tfidfs.foreach(x => println(x._1 + ": " + x._2))
	
	sc.parallelize(tfidfs).map(_.productIterator.mkString("\t")).coalesce(1).saveAsTextFile(path + "tf_idf_results.tsv")
	
    println("-end-")
	
	sc.stop()
  }
}
