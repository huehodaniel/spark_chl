import org.apache.spark.{SparkConf, SparkContext}

object NasaApp {
  def main(args: Array[String]) {
    // Setup context
    val logFile = sys.env("SPARK_CHL_FILE")
    val conf = new SparkConf().setMaster("local").setAppName("SparkChl")
    val sc = new SparkContext(conf)

    // Load log data
    val logData = sc.textFile(logFile)

    // We need a regex to parse the line
    // ex: 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    // Note: there is some bad data in the log, for some reason. Filter them out.
    val lineRegex = """(.+) - - \[(.+)\] "(.+)" (\d+) (.+)""".r
    val parsedRDD = logData.flatMap {
      case lineRegex(host, timestamp, request, status, reply) => Some((host, timestamp, request, status, reply))
      case _ => None
    }

    // Test cache command
    val formattedRDD = sys.env.get("SPARK_CHL_CACHE") match {
      case Some(_) => parsedRDD.cache()
      case None => parsedRDD
    }

    // Number of unique hosts
    // Probably bad performance (?)
    val uniqueHosts = formattedRDD.map(_._1).distinct().count()

    // Fetch 404s
    val notFoundRDD = formattedRDD.filter(_._4 == "404")

    // Number of 404s
    val notFoundCount = notFoundRDD.count()

    // We need to parse requests for the next one
    val requestRegex = """\w+ ([\*\~\%\.\:\w+/-]+).*""".r

    // Top 5 404s
    val topFiveNotFound = notFoundRDD.map(_._3).map {
      case requestRegex(url) => (url, 1)
      case any =>
        throw new RuntimeException(s"[BUG! topFiveNotFound] $any")
    }
      .reduceByKey(_ + _)
      .top(5)(Ordering.by(_._2))

    // For this one, we need to parse the timestamps as dates
    val timeRegex = """(\d+/\w+/\d+):.+""".r
    val notFoundPerDay = notFoundRDD.map(_._2).map {
      case timeRegex(date) => (date, 1)
      case any =>
        throw new RuntimeException(s"[BUG! notFoundPerDay] $any")
    }
      .reduceByKey(_ + _)
      .collect()

    // Total traffic from replies
    val totalTraffic = formattedRDD.map {
      case (_, _, _, _, "-") => 0L
      case (_, _, _, _, str) => str.toLong
    }.reduce(_ + _)

    println(s"Unique hosts: $uniqueHosts")
    println(s"404 errors total count: $notFoundCount")

    val formattedTopFive = topFiveNotFound.map {
      case (url, count) => s"- $url: $count"
    }.mkString("\n")
    println(s"Top 5 not found URLs:\n$formattedTopFive")

    val formattedNotFoundPerDay = notFoundPerDay.map {
      case (date, count) => s"- $date: $count"
    }.sorted.mkString("\n")
    println(s"404's per day:\n$formattedNotFoundPerDay")

    println(s"Total traffic from replies: $totalTraffic bytes")

    sc.stop()
  }
}
