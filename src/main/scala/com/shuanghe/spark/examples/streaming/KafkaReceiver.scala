package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * receiver接收kafka数据
  */
object KafkaReceiver {
    def main(args: Array[String]) {
        //        if (args.length < 4) {
        //            System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
        //            System.exit(1)
        //        }

        Logger.getRootLogger.setLevel(Level.INFO)

        //        val Array(zkQuorum, group, topics, numThreads) = args
        val zkQuorum = "shuanghe.com:2181"
        val group = "defaultConsumerGroup"
        val topics = "test"
        val numThreads = 1

        val spark = SparkSession.builder()
                .appName("KafkaWordCount")
                .master("local[2]")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("KafkaWordCount")
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        ssc.checkpoint("spark/streaming/checkpoint")

        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

        val lines: DStream[String] = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
                .map(_._2)

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
