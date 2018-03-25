package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * receiver接收kafka数据
  */
@deprecated
object KafkaReceiver {
    def main(args: Array[String]) {
        if (args.length < 4) {
            System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        val Array(zkQuorum, group, topics, numThreads) = args

        val spark = SparkSession.builder()
                .appName("KafkaWordCount")
                .master("local")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("KafkaWordCount")
        val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

        ssc.checkpoint("checkpoint")

        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

        //        val words = lines.flatMap(_.split(" "))
        //        val wordCounts = words.map(x => (x, 1L))
        //                .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words
                .map(x => (x, 1L))
                .reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
