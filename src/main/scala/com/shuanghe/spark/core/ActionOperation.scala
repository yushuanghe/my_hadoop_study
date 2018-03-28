package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ActionOperation {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("ActionOperation")
                .master("local")
                .getOrCreate()

        val list: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val rdd: RDD[Int] = spark.sparkContext.parallelize(list, 2)

        //        reduce(rdd)
        //        collect(rdd)
        //        count(rdd)
        //        take(rdd)
        //        saveAsTextFile(rdd)
        countByKey(spark)

        spark.close()
    }

    def reduce(rdd: RDD[Int]): Unit = {
        val a: Int = rdd.reduce(_ * _)
        println(a)
    }

    def collect(rdd: RDD[Int]): Unit = {
        val a: Array[Int] = rdd.map(_ * 2).collect()

        a.foreach(println(_))
    }

    def count(rdd: RDD[Int]): Unit = {
        val a: Long = rdd.count()

        println(a)
    }

    def take(rdd: RDD[Int]): Unit = {
        val a: Array[Int] = rdd.take(3)

        a.foreach(println(_))
    }

    def saveAsTextFile(rdd: RDD[Int]): Unit = {
        rdd.saveAsTextFile("spark/saveAsTextFile")
    }

    def countByKey(spark: SparkSession): Unit = {
        val scoreList: ArrayBuffer[(Int, Int)] = ArrayBuffer((1, 100), (2, 99), (3, 98), (1, 96), (1, 95))

        val rdd: RDD[(Int, Int)] = spark.sparkContext.parallelize(scoreList, 2)

        val map: collection.Map[Int, Long] = rdd.countByKey()

        for (e <- map) {
            println(e)
        }
    }
}
