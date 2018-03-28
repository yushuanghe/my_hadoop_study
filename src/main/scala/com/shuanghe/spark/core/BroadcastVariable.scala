package com.shuanghe.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BroadcastVariable {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("BroadcastVariable")
                .master("local")
                .getOrCreate()

        val rdd: RDD[Int] = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)

        val factor = 3
        val broadcast: Broadcast[Int] = spark.sparkContext.broadcast(factor)

        rdd.map(_ * broadcast.value).foreach(println(_))

        spark.close()
    }
}
