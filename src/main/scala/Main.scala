import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    //spark session init
    val spark = SparkSession.builder()
    .appName("RedditNER")
    .master("local[*]")
    .getOrCreate()
    val sc = spark.sparkContext

    // Acumuladores
    val feedsSuccessAcc = sc.longAccumulator("FeedsSuccess")
    val feedsFailedAcc = sc.longAccumulator("FeedsFailed")
    val postsTotalAcc = sc.longAccumulator("PostsTotal")
    val postsFilteredAcc = sc.longAccumulator("PostsFiltered")

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
    val downloadResultsRDD = subscriptionsRDD.flatmap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt match {
        case Some(content) => 
          feedsSuccessAcc.add(1)
          val parsedPosts = JsonParser.parsePosts(content, subscription.name, subscription.url)
          postsTotalAcc.add(parsedPosts.length)
          parsedPosts
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          feedsFailedAcc.add(1)
          List.empty[Post]
      }
      posts
    }

    // filter: descarta posts vacíos e incrementa postsEmptyAcc por cada uno
    val filteredPostsRDD = downloadResultsRDD.filter { post =>
      val isEmpty = post.title.isEmpty && post.selftext.isEmpty
      if (isEmpty) postsFilteredAcc.add(1)
      !isEmpty
    }

    val filteredPosts = filteredPostsRDD.collect().toList

    // Calculate average characters in filtered posts
    val avgChars =
      if (filteredPosts.nonEmpty)
        filteredPosts.map(p => p.title.length + p.selftext.length).sum / filteredPosts.length
      else 0L
    
    // Prepare statistics (.int o cambiar el parametro esperado?)
    val stats = Map(
      "feedsSuccess"  -> feedsSuccessAcc.value.toInt,
      "feedsFailed"   -> feedsFailedAcc.value.toInt,
      "postsSuccess"  -> postsTotalAcc.value.toInt,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
