package com.shuanghe.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.sys.exit

/**
  * 根据第一个字段分组，根据第二个字段排序
  */
object SparkGroupByKey {
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("Usage: JavaWordCount <file>")
            exit(1)
        }

        val spark = SparkSession
                .builder()
                .appName("groupSort")
                //                .master("local")
                .getOrCreate()

        val rdd: RDD[String] = spark.sparkContext.textFile(args(0), 1)

        val groupRdd: RDD[(String, Iterable[Int])] = rdd
                .map { line => {
                    val strs = line.split(" ")
                    (strs(0), strs(1).toInt)
                }
                }
                .groupByKey().sortByKey()

        val groupSortRdd = groupRdd
                .map { tuple => {
                    var list = tuple._2.toList
                    val value = list.sorted.takeRight(3).reverse
                    (tuple._1, value)
                }
                }

        val outputFile = "spark/groupSort"
        groupSortRdd.foreach(println(_))
        groupSortRdd.saveAsTextFile(outputFile)

        spark.close()
    }
}
