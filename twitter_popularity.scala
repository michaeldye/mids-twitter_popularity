import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.util.matching.Regex
import org.apache.spark.storage.StorageLevel

object Main extends App {
  if (args.size != 3) {
    usage()
    System.exit(1)
  }

  val totalRuntime = args(0).toInt
  val samplingInterval = args(1).toInt
  val filterCutoff = args(2).toInt

  var out =  Array[(Int, (String, Set[String]))]()

  val propPrefix = "twitter4j.oauth."
  System.setProperty(s"${propPrefix}consumerKey", "")
  System.setProperty(s"${propPrefix}consumerSecret", "")
  System.setProperty(s"${propPrefix}accessToken", "")
  System.setProperty(s"${propPrefix}accessTokenSecret", "")

  // Create a streaming context w/ batch duration 5s
  val ssc = new StreamingContext(new SparkConf().setAppName("twitter popularity"), Seconds(5))

  // Open the Twitter stream
  val stream = TwitterUtils.createStream(ssc, None)

  // build a DStream of RDDs that are tuples of (hashTag, Set[String]); where *every* hashtag extracted from the tweet text is paired with a set of every user associated with that hashTag (both the author and mentioned users)
  val tweetData = stream.flatMap(status => {

    // build the Set of users (String type) by extracting references from the tweet text and adding String objects for each; add also the author
    val users: Set[String] = extractFromText("\\S*@(\\w\\S+)".r, status.getText).map { name => name } + justText(status.getUser.getScreenName)

    // extract hashtag text (may be multiple) from the tweet text and output tuple
    extractFromText("\\S*#(\\w\\S+)".r, status.getText).map(hashTag => {
      (hashTag, users)
    })
  })

  // will run at batch duration, output some stats here to keep things interesting
  tweetData.foreachRDD(rdd => {
    val uniqueCount = rdd.map({
      case (hashTag, _) => hashTag
    }).distinct().count()

    if (!rdd.isEmpty) {
      println(s"In last sampling interval processed ${rdd.count()} hashTags, $uniqueCount of which were unique.")

      val sampled = rdd.sample(false, .5).take(8)
      println(s"Sample input (note that this sampling could contain duplicate tags; these are combined later):\n${sampled mkString "\n"}\n")
    }
  })

  // combine up tuples w/ same hashTag; this is important b/c during a given sampling period the same hashTag can be received by one or more workers processing a stream
  tweetData.window(Seconds(totalRuntime), Seconds(samplingInterval)).combineByKey(
    (users: Set[String]) => (users, 1),
    (combiner: (Set[String], Int), users: Set[String]) => (combiner._1 ++ users, combiner._2 + 1),
    (comb1: (Set[String], Int), comb2: (Set[String], Int)) => (comb1._1 ++ comb2._1, comb1._2 + comb2._2),
    new org.apache.spark.HashPartitioner(10 / 2))
  .map({ case (tag, (users, count)) => (count, (tag, users)) })
  .transform(_.sortByKey(false))
  .foreachRDD(rdd => {
    // action: prep final results, save in mutable var in driver for convenient output after processing completes
    out = rdd.collect().filter(_._1 >= filterCutoff)
  })

  /** actually starts execution, everything prior should be thought of as "registering" operations w/ Spark **/
  ssc.start()
  ssc.awaitTerminationOrTimeout((totalRuntime + samplingInterval) * 1000)
  ssc.stop(true, true)

  /** final output after processing has stopped **/
  println(s"Top aggregate results sampled over ${totalRuntime}s:\n${out mkString "\n"}")
  println(s"Exiting.")

  /** functions **/
  def extractFromText(pattern: Regex, text: String): Set[String] = pattern.findAllIn(text).matchData.map { it =>
    justText(it.group(1).mkString)
  }.toSet

  def justText(str: String): String = str.replaceAll("""[\p{Punct}]""","")

  def usage(): Unit = println("""Error in CLI invocation.
Usage: $SPARK_HOME/bin/spark-submit ... "jarfile" "total runtime in s" "sampling duration in s" "count cutoff (inclusive)"

Note that the sampling duration is a sliding interval in this implementation and so must evenly divide the total runtime.""")
}
