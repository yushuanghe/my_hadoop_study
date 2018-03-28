package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object TextFile {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("TextFile")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("spark/input/spark.txt", 3)

        val lengthRdd: RDD[Int] = rdd.map(x => x.length)

        val sum: Int = lengthRdd.reduce(_ + _)

        println(sum)

        spark.close()
    }
}
