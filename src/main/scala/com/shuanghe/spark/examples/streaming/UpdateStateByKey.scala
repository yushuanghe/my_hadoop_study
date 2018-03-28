package com.shuanghe.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 请注意，使用updateStateByKey需要配置检查点目录
  */
object UpdateStateByKey {
    def main(args: Array[String]): Unit = {
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

        val spark = SparkSession.builder()
                .appName("testUpdateStateByKey")
                .master("local")
                .getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        ssc.checkpoint("checkpoint")

        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

        val lines: DStream[String] = messages.map(_._2)

        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordStream: DStream[(String, Int)] = words.map((_, 1))

        //values:Seq是本次数据，state:Option是之前的状态
        //updateFunc: (Seq[V], Option[S]) => Option[S]
        val updateFunc =
        (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.sum

            val previousCount = state.getOrElse(0)

            Some(currentCount + previousCount)
        }

        val update: DStream[(String, Int)] = wordStream.updateStateByKey[Int](updateFunc)

        update.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
