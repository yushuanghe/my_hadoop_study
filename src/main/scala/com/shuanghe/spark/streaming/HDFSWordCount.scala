package com.shuanghe.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("HDFSWordCount")
                .master("local[2]")
                .getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        val lines: DStream[String] = ssc.textFileStream("hdfs://shuanghe.com:8020/user/yushuanghe/spark/wordcount_dir")

        val words: DStream[String] = lines.flatMap((line: String) => line.split(" "))

        val pairs: DStream[(String, Int)] = words.map((word: String) => (word, 1))

        val wordcounts: DStream[(String, Int)] = pairs.reduceByKey((v1: Int, v2: Int) => v1 + v2)

        wordcounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
