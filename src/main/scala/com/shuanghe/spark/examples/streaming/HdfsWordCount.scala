package com.shuanghe.spark.examples.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从hdfs读取文件（监听新文件）
  * 结果写入到hdfs中
  */
object HdfsWordCount {
    def main(args: Array[String]) {
        if (args.length < 1) {
            System.err.println("Usage: HdfsWordCount <directory>")
            System.exit(1)
        }

        Logger.getRootLogger.setLevel(Level.INFO)

        val spark = SparkSession.builder()
                .appName("HdfsWordCount")
                .master("local")
                .getOrCreate()
        //        val sparkConf = new SparkConf().setAppName("HdfsWordCount")
        // Create the context
        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        // Create the FileInputDStream on the directory and use the
        // stream to count words in new files created
        val lines = ssc.textFileStream(args(0))

        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        //结果写入到hdfs中
        //结果写入到:spark/streaming/saveToHdfs-1521971170000.txt/ 目录中
        wordCounts.saveAsTextFiles("spark/streaming/saveToHdfs", "txt")

        ssc.start()
        ssc.awaitTermination()
    }
}
