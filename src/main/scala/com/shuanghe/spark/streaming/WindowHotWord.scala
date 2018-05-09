package com.shuanghe.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowHotWord {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("WindowHotWord")
                .master("local[2]")
                .getOrCreate()

        val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        ssc.checkpoint("spark/streaming/checkpoint")

        val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> "shuanghe.com:9092")

        val topics: Set[String] = Set[String]("test")

        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val searchLogsDStream: DStream[String] = messages.map((tuple: (String, String)) => tuple._2)

        val searchWordsDStream: DStream[String] = searchLogsDStream.map((log: String) => log.split(" ")(1))

        val searchWordPairDStream: DStream[(String, Int)] = searchWordsDStream.map((word: String) => (word, 1))

        val searchWordCountsDStream: DStream[(String, Int)] = searchWordPairDStream.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2
            , Seconds(60), Seconds(10))

        val finalDStream: DStream[(String, Int)] = searchWordCountsDStream.transform {
            (searchWordCountsRDD: RDD[(String, Int)]) => {
                val countSearchWordsRDD: RDD[(Int, String)] = searchWordCountsRDD.map {
                    case (word: String, count: Int) => (count, word)
                }

                val sortedCountSearchWordsRDD: RDD[(Int, String)] = countSearchWordsRDD.sortByKey(ascending = false)

                val sortedSearchWordCountsRDD: RDD[(String, Int)] = sortedCountSearchWordsRDD.map {
                    case (count: Int, word: String) => (word, count)
                }

                spark.sparkContext.parallelize(sortedSearchWordCountsRDD.take(3))
            }
        }

        finalDStream.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
