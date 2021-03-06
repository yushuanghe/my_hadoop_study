package com.shuanghe.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * kafka direct api拉取数据
  */
object DirectKafkaWordCount {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println(
                s"""
                   |Usage: DirectKafkaWordCount <brokers> <topics>
                   |  <brokers> is a list of one or more Kafka brokers
                   |  <topics> is a list of one or more kafka topics to consume from
                   |
        """.stripMargin)
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        val Array(brokers, topics) = args

        // Create context with 2 second batch interval
        val spark = SparkSession.builder()
                .appName("DirectKafkaWordCount")
                .master("local")
                .getOrCreate()

        //        val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        //shuanghe.com:9092
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topicsSet)

        // Get the lines, split them into words, count the words and print
        val lines = messages.map(_._2)

        val words = lines.flatMap(_.split(" "))
        val wordCounts = words
                .map(x => (x, 1L))
                .reduceByKey(_ + _)

        wordCounts.print()

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }
}
