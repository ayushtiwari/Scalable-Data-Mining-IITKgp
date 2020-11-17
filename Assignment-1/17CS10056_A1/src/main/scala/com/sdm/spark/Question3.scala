package com.sdm.spark

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Question3 {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local").setAppName("Gamma")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val processLine1 = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r
    val dateType1 = "yyyy-MM-dd:HH:mm:ss"
    val dateFormat1 = new SimpleDateFormat(dateType1)

    val logsRdd = sc.
      textFile("/Users/ayushtiwari/Downloads/ghtorrent-logs.txt").
      flatMap ( _ match {
        case processLine1(debugLevel, date, downloadId, retrievalStage, rest) =>
          Some(GitLog(debugLevel, dateFormat1.parse(date.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
        case _ => None
      })

    val dateType2 = "yyyy-MM-dd HH:mm:ss"
    val processLine2 = """([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)""".r
    val dateFormat2 = new SimpleDateFormat(dateType2)

    val infoRdd = sc.
      textFile("/Users/ayushtiwari/Downloads/important-repos.csv").
      mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter).
      flatMap(_ match {
        case processLine2(id, url, ownerId, name, language, createdAt, forkedFrom, deleted, updatedAt) => {
          Some(Repository(id.toInt, url, ownerId.toInt, name, language, dateFormat2.parse(createdAt), forkedFrom, deleted.toInt, dateFormat2.parse(updatedAt)))
        }
        case _ => None
      })

    println("--------------------------------------")
    println("Read in the file to an RDD and name it as ‘assignment_2’ and " +
      "\ncount the number of records.")
    println(infoRdd.count())

    println("--------------------------------------")
    println("How many records in the log file (used in the last 2 questions) " +
      "\nrefer to entries in the 'assignment_2' file ?")
    val info = infoRdd.keyBy(_.name)
    val logs = logsRdd.keyBy(_.rest).
      map(record => record.copy(_1 = record._1.split("/").slice(4,6).mkString("/").takeWhile(_ != '?').split("/", 2).last)).
      filter(_._1.nonEmpty)
    val joined = info.join(logs)
    println(joined.count())

    println("--------------------------------------")
    println("Which of the ‘assignment_2’ repositories has the most failed API calls?")
    joined.filter(record => record._2._2.rest.contains("Failed")).
      map(record => (record._1, 1)).
      reduceByKey(_ + _).
      sortBy(x => x._2, false).
      take(1).
      foreach(println)
  }
}
