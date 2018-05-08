package com.shuanghe.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlacklist {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("TransformBlacklist")
                .master("local[2]")
                .getOrCreate()

        val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

        ssc.checkpoint("spark/streaming/checkpoint")

        val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> "shuanghe.com:9092")
        val topics: Set[String] = Set[String]("test")
        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        val logs: DStream[String] = messages.map((tuple: (String, String)) => tuple._2)

        //黑名单
        val blacklistData = Array[(String, Boolean)](("tom", true))
        val blacklistRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacklistData)

        val userAdsClickLogDStream: DStream[(String, String)] = logs.map((log: String) => (log.split(" ")(1), log))

        val validAdsClickLogDStream: DStream[String] = userAdsClickLogDStream.transform[String] {
            (userAdsClickLogRDD: RDD[(String, String)]) => {
                val joinedRDD: RDD[(String, (String, Option[Boolean]))] = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)

                val filteredRDD: RDD[(String, (String, Option[Boolean]))] = joinedRDD.filter {
                    case (key: String, (log: String, flag: Option[Boolean])) =>
                        !flag.getOrElse(false)
                }

                val validAdsClickLogRDD: RDD[String] = filteredRDD.map {
                    case (key: String, (log: String, flag: Option[Boolean])) =>
                        log
                }

                validAdsClickLogRDD
            }
        }

        validAdsClickLogDStream.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
