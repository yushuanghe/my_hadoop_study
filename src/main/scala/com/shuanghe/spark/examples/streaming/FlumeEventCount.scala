package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * flume推数据
  */
object FlumeEventCount {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println(
                "Usage: FlumeEventCount <host> <port>")
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        val Array(host, port) = args

        val batchInterval = Milliseconds(5000)

        // Create the context and set the batch size
        val spark = SparkSession.builder()
                .appName("FlumeEventCount")
                .master("local")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("FlumeEventCount")
        val ssc = new StreamingContext(spark.sparkContext, batchInterval)

        // Create a flume stream
        val stream = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

        // Print out the count of events received from this server in each batch
        stream.count()
                .map(cnt => "Received " + cnt + " flume events.")
                .print()

        ssc.start()
        ssc.awaitTermination()
    }
}
