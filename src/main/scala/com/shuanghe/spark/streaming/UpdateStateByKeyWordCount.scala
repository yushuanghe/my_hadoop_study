package com.shuanghe.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyWordCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("UpdateStateByKeyWordCount")
                .master("local[2]")
                .getOrCreate()

        val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        ssc.checkpoint("spark/streaming/checkpoint")

        val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> "shuanghe.com:9092")

        val topics = Set[String]("test")

        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val lines: DStream[String] = messages.map((tuple: (String, String)) => tuple._2)

        val wordcounts: DStream[(String, Int)] = lines.flatMap((line: String) => line.split(" "))
                .map((word: String) => (word, 1))

        val result: DStream[(String, Int)] = wordcounts.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
            var newValue = state.getOrElse(0)

            for (value <- values) {
                newValue += value
            }

            Option(newValue)
        })

        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
