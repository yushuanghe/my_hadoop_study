package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SortWordCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("SortWordCount")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/spark.txt", 2)

        val list: Array[(String, Int)] = rdd.flatMap(_.split(" "))
                .map((_, 1))
                .reduceByKey(_ + _)
                .map(x => (x._2, x._1))
                .sortByKey(ascending = false)
                .map(x => (x._2, x._1))
                .take(5)

        list.foreach(println(_))

        spark.close()
    }
}
