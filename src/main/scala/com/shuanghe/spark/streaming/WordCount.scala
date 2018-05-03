package com.shuanghe.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("WordCount")
                .master("local[2]")
                .getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val words: DStream[String] = lines.flatMap((line: String) => {
            line.split(" ")
        })

        val pairs: DStream[(String, Int)] = words.map((word: String) => {
            (word, 1)
        })

        val wordCounts = pairs.reduceByKey((v1: Int, v2: Int) => {
            v1 + v2
        })

        wordCounts.foreachRDD((rdd: RDD[(String, Int)]) => {
            rdd.foreach(println(_))
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
