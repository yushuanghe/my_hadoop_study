package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: NetworkWordCount <hostname> <port>")
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        // Create the context with a 1 second batch size
        val spark = SparkSession.builder()
                .appName("NetworkWordCount")
                .master("local")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("NetworkWordCount")
        //        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        // Create a socket stream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordCounts: DStream[(String, Int)] = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
