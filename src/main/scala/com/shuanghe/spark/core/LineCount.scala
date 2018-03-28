package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LineCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("LineCount")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("mapreduce/input", 1)

        val pairs: RDD[(String, Int)] = rdd.map((_, 1))

        val counts: RDD[(String, Int)] = pairs.reduceByKey(_ + _)

        counts.foreach(println(_))

        spark.close()
    }
}
