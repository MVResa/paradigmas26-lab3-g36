import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    //spark session init
    val spark = SparkSession.builder()
    .appName("RedditNER")
    .master("local[*]")
    .getOrCreate()
    val sc = spark.sparkContext


    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatMap {
      case Some(sub) => Some(sub)
      case None =>
        println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
        None
    }

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    //paralelize subs 
    val subscriptionsRDD = sc.parallelize(subscriptions)
    
    // Download feeds and parse posts, tracking success/failure
    /* val downloadResults = subscriptions.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      (feedOpt.isDefined, posts)
    } */
    // FlatMap para obtener el RDD[Post]
    val downloadResultsRDD = subscriptionsRDD.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt match {
        case Some(content) => 
          JsonParser.parsePosts(content, subscription.name, subscription.url)
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List[Post]()
      }
      (feedOpt.isDefined, posts)
    }.collect().toList

    //change downloadResultsRDD for downloadResults for original structure

    // Count feed successes/failures
    val feedsSuccess = downloadResultsRDD.count(_._1)
    val feedsFailed = downloadResultsRDD.length - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResultsRDD.flatMap(_._2)
    val postsSuccess = allPosts.length
    val postsFailed = downloadResultsRDD.count(_._2.isEmpty)

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = allPosts.length - filteredPosts.length

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

// list posts in RDD
    val RDDPosts = sc.parallelize(filteredPosts);

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = RDDPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // cuenta el numero de entidades por tipo y nombre
    val entityPairsRDD = allEntities.map(entity => ((entity.entityType, entity.text), 1))
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)
    val entityCounts = entityCountsRDD.collect().toMap

    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
