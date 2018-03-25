package com.shuanghe.spark.examples

import org.apache.spark.sql.SparkSession

import scala.sys._

object SparkWordCount {
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("Usage: JavaWordCount <file>")
            exit(1)
        }

        val spark = SparkSession
                .builder()
                .appName("wordcount")
                .master("local")
                .getOrCreate()

        val wordcount = spark.read.textFile(args(0)).rdd.repartition(1)
                .flatMap(_.split(" "))
                .map((_, 1))
                .reduceByKey(_ + _)

        val topn = wordcount
                .map(x => (x._2, x._1))
                .sortByKey(ascending = false)
                .map(x => (x._2, x._1))

        //        topn.saveAsTextFile("/user/yushuanghe/spark/output2")

        topn.take(5).foreach(println(_))

        spark.close()
    }
}
