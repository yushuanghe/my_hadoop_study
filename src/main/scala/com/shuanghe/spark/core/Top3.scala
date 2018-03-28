package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Top3 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("Top3")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/top.txt", 2)

        val result: Array[String] = rdd.map(x => (x.toInt, x))
                .sortByKey(ascending = false)
                .map(_._2)
                .take(3)

        result.foreach(println(_))

        spark.close()
    }
}
