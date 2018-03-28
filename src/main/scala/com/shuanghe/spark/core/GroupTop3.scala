package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GroupTop3 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("Top3")
                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/score.txt", 2)

        val groupRdd: RDD[(String, Iterable[Int])] = rdd.map { line => {
            val strs: Array[String] = line.split(" ")
            (strs(0), strs(1).toInt)
        }
        }.groupByKey()

        val resultRdd: RDD[(String, List[Int])] = groupRdd.map { tuple => {
            val list: List[Int] = tuple._2.toList

            val value: List[Int] = list.sorted.reverse.take(3)
            (tuple._1, value)
        }
        }

        resultRdd.foreach(println(_))

        spark.close()
    }
}
