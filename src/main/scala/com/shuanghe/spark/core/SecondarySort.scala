package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SecondarySort {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("SortWordCount")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/sort.txt", 2)

        val resultRdd: RDD[String] = rdd.map { line => {
            val strs: Array[String] = line.split(" ")
            (new SecondarySortKey(strs(0).toInt, strs(1).toInt), line)
        }
        }.sortByKey()
                .map(_._2)

        resultRdd.foreach(println(_))

        spark.close()
    }
}
