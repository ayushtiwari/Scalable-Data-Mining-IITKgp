package com.sdm.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD

object Question2 {

  def inverted_index(rdd : RDD[GitLog], id : Int): RDD[(Any, Iterable[GitLog])] = {
    rdd.groupBy(_.productElement(id))
  }

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
    println("b) Compute the number of different repositories accessed " +
      "\nby the client ‘ghtorrent-22’ (without using the inverted index).")
    val client22 = rdd.filter(_.downloadId.equals(22)).
      filter(_.retrievalStage.equals("api_client")).
      map(_.rest.split("/").slice(4,6).mkString("/").takeWhile(! _.equals('?')))

    println(client22.distinct.count())

    println("--------------------------------------")
    println("c) Compute the number of different repositories accessed " +
      "\nby the client ‘ghtorrent-22’ using the inverted index calculated above.")
    val invertedIndex = inverted_index(rdd, 2)

    var repos = List[String]()
    for (list <- invertedIndex.lookup(22))
      for (item <- list)
        if (item.retrievalStage.equals("api_client"))
          repos = repos :+ item.rest.split("/").slice(4, 6).mkString("/").takeWhile(! _.equals('?'))

    println(repos.distinct.length)

  }
}
