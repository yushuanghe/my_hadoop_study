package com.shuanghe.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object TransformationOperation {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("LineCount")
                .master("local")
                .getOrCreate()

        val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)

        //        map(rdd)
        //        filter(rdd)
        //        flatMap(rdd)
        //        groupByKey(spark)
        //        reduceByKey(spark)
        //        sortByKey(spark)
        join(spark)
        cogroup(spark)
        spark.close()
    }

    def map(rdd: RDD[Int]): Unit = {
        rdd.map(_ * 2).foreach(println(_))
    }

    def filter(rdd: RDD[Int]): Unit = {
        rdd.filter(_ % 2 == 1).foreach(println(_))
    }

    def flatMap(rdd: RDD[Int]): Unit = {
        rdd.flatMap { x =>
            var list = ArrayBuffer[String]()
            for (i <- 0.until(x)) {
                list += x + ":" + i
            }
            list
        }.foreach(println(_))
    }

    def groupByKey(spark: SparkSession): Unit = {
        val scores = Array(("class1", 80), ("class2", 81), ("class1", 83), ("class2", 84))

        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(scores)

        val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        groupRdd.foreach { tuple =>
            println(tuple._1)

            for (i <- tuple._2) {
                println(i)
            }

            println("=================")
        }
    }

    def reduceByKey(spark: SparkSession): Unit = {
        val scores = Array(("class1", 80), ("class2", 81), ("class1", 83), ("class2", 84))

        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(scores)

        val redueRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

        redueRdd.foreach(println(_))
    }

    def sortByKey(spark: SparkSession): Unit = {
        val scores = Array(("class1", 80), ("class2", 81), ("class1", 83), ("class2", 84))

        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(scores)

        val sortRdd: RDD[(String, Int)] = rdd.map(x => (x._2, x._1))
                .sortByKey(ascending = false)
                .map(x => (x._2, x._1))

        sortRdd.foreach(println(_))
    }

    def join(spark: SparkSession): Unit = {
        val scoreList: ArrayBuffer[(Int, Int)] = ArrayBuffer((1, 100), (2, 99), (3, 98), (1, 96), (1, 95))
        val studentList: ArrayBuffer[(Int, String)] = ArrayBuffer((1, "leo"), (2, "jack"), (3, "dali"), (4, "haha"))

        val studentRdd = spark.sparkContext.parallelize(studentList, 2)
        val scoreRdd = spark.sparkContext.parallelize(scoreList, 2)

        val rdd: RDD[(Int, (String, Int))] = studentRdd.join(scoreRdd)

        rdd.foreach(println(_))
    }

    def cogroup(spark: SparkSession): Unit = {
        val scoreList: ArrayBuffer[(Int, Int)] = ArrayBuffer((1, 100), (2, 99), (3, 98), (1, 96), (1, 95))
        val studentList: ArrayBuffer[(Int, String)] = ArrayBuffer((1, "leo"), (2, "jack"), (3, "dali"), (4, "haha"))

        val studentRdd = spark.sparkContext.parallelize(studentList, 2)
        val scoreRdd = spark.sparkContext.parallelize(scoreList, 2)

        val rdd: RDD[(Int, (Iterable[String], Iterable[Int]))] = studentRdd.cogroup(scoreRdd)

        rdd.foreach(println(_))
    }
}
