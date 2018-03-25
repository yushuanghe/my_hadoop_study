package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * spark从flume拉数据(有问题)
  */
@deprecated
object FlumePollingEventCount {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println(
                "Usage: FlumePollingEventCount <host> <port>")
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        val Array(host, port) = args

        val batchInterval = Milliseconds(5000)

        // Create the context and set the batch size
        val spark = SparkSession.builder()
                .appName("FlumePollingEventCount")
                .master("local")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
        val ssc = new StreamingContext(spark.sparkContext, batchInterval)

        // Create a flume stream that polls the Spark Sink running in a Flume agent
        val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

        // Print out the count of events received from this server in each batch
        val count = stream.count()
                .map(cnt => "Received " + cnt + " flume events.")

        count.print()
        count.saveAsTextFiles("spark/streaming/pullFlume")

        ssc.start()
        ssc.awaitTermination()
    }
}
