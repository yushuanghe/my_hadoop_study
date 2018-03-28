package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ParallelizeCollection {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("ParallelizeCollection")
                .master("local")
                .getOrCreate()

        val numbers: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        val rdd: RDD[Int] = spark.sparkContext.parallelize(numbers, 5)

        val sum: Int = rdd.reduce(_ + _)

        println(sum)

        spark.close()
    }
}
