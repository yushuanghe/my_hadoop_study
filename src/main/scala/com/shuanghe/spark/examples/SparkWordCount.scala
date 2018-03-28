package com.shuanghe.spark.examples

import org.apache.spark.rdd.RDD
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

        val wordcount: RDD[(String, Int)] = spark.sparkContext.textFile(args(0))

                .flatMap(_.split(" "))
                .map((_, 1))
                //reduceByKey会有本地combiner优化
                .reduceByKey(_ + _)

        val topn: RDD[(String, Int)] = wordcount
                .map(x => (x._2, x._1))
                .sortByKey(ascending = false)
                .map(x => (x._2, x._1))

        //        topn.saveAsTextFile("/user/yushuanghe/spark/output2")

        topn
                //                .take(5)
                .foreach(println(_))

        spark.close()
    }
}
