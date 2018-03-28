package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object AccumulatorVariable {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("AccumulatorVariable")
                .master("local")
                .getOrCreate()

        val list: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val rdd: RDD[Int] = spark.sparkContext.parallelize(list, 2)

        val accum: LongAccumulator = spark.sparkContext.longAccumulator("longAccum")

        val rdd1: RDD[Int] = rdd.map { x =>
            accum.add(x)
            x + 1
        }

        rdd1.cache().count()
        println(accum.value)
        rdd1.count()
        println(accum.value)

        spark.close()
    }
}
