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
    val downloadResultsRDD = subscriptionsRDD.flatMap { subscription =>
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

    val cachedFPostRDD = filteredPostsRDD.cache()

    val t0_descarga = System.currentTimeMillis()
    val postCount = cachedFPostRDD.count()
    val t1_descarga = System.currentTimeMillis()

    // Calculate average characters in filtered posts
    val avgChars =
      if (postCount > 0)
        val totalChars = cachedFPostRDD.map(p => p.title.length + p.selftext.length.toLong).reduce( _ + _ )
        totalChars / postCount
      else 0L
    
    // Prepare statistics (.int o cambiar el parametro esperado?)
    val stats = Map(
      "feedsSuccess"  -> feedsSuccessAcc.value.toInt,
      "feedsFailed"   -> feedsFailedAcc.value.toInt,
      "postsSuccess"  -> postsTotalAcc.value.toInt,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Check if we have any posts to process
    if (postCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      cachedFPostRDD.unpersist()
      return
    }

// list posts in RDD
    //val RDDPosts = sc.parallelize(filteredPosts);

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = cachedFPostRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // cuenta el numero de entidades por tipo y nombre
    val entityPairsRDD = allEntities.map(entity => ((entity.entityType, entity.text), 1))
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)
    val t0_ner = System.currentTimeMillis()
    val entityCounts = entityCountsRDD.collect().toMap
    val t1_ner = System.currentTimeMillis()

    //liberar cache antes de terminar
    cachedFPostRDD.unpersist()

    // Medimos los tiempos
    val durationDescarga = (t1_descarga - t0_descarga) / 1000.0
    val durationNER = (t1_ner - t0_ner) / 1000.0
    
    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
    println()
    println(Formatters.formatExecutionTimes(durationDescarga, durationNER))
    println()
  }
}
