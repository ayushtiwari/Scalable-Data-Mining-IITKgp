package com.sdm.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

object Question1 {

  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local").setAppName("Gamma")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val processLine = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r
    val dateType = "yyyy-MM-dd:HH:mm:ss"
    val dateFormat = new SimpleDateFormat(dateType)

    val rdd = sc.
      textFile("/Users/ayushtiwari/Downloads/ghtorrent-logs.txt").
      flatMap ( _ match {
        case processLine(debugLevel, date, downloadId, retrievalStage, rest) =>
          Some(GitLog(debugLevel, dateFormat.parse(date.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
        case _ => None
      })

    println("--------------------------------------")
    println("a) How many lines does the RDD contain?")
    println(rdd.count)

    println("--------------------------------------")
    println("b) Count the number of “WARN” messages.")
    println(rdd.filter(_.debugLevel.equals("WARN")).count)

    println("--------------------------------------")
    println("c) How many repositories were processed in " +
      "\ntotal when the retrieval_stage is 'api_client'?")
    val repos = rdd.filter(_.retrievalStage == "api_client").
      map(_.rest.split("/").slice(4,6).mkString("/").takeWhile(_ != '?'))
    println(repos.distinct.count())

    println("--------------------------------------")
    println("d) Using retrieval_stage as “api_client”, " +
      "\nfind which clients did the most HTTP requests and " +
      "\nFAILED HTTP requests from the download_id field?")
    println("Most HTTP Requests")
    println("(downloadId, count)")
    rdd.filter(_.retrievalStage.equals("api_client")).
      keyBy(_.downloadId).
      mapValues(_ => 1).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      foreach(println)
    println("Most Failed Requests")
    println("(downloadId, count)")
    rdd.filter(_.retrievalStage.equals("api_client")).
      filter(_.rest.contains("Failed")).
      keyBy(_.downloadId).
      mapValues(_ => 1).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      foreach(println)

    println("--------------------------------------")
    println("d) Find the most active hour of the day and most active repository?")
    println("Most Active Hour of Day")
    println("(hour, count)")
    rdd.keyBy(_.timeStamp.getHours).
      mapValues(_ => 1).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      take(1).
      foreach(println)
    println("Most Active Repository")
    println("(name, count)")
    repos.filter(_.nonEmpty).
      map((_, 1)).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      take(1).
      foreach(println)

    println("--------------------------------------")
    println("e) Which access keys are failing most often?")
    rdd.filter(x => x.rest.contains("Failed") && x.rest.contains("Access: ")).
      map(x => (x.rest.split("Access: ", 2)(1).split(",", 2)(0), 1)).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      take(10).
      foreach(println)
  }
}
