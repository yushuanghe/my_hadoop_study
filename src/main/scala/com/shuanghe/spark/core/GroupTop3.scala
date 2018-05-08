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

        val resultRdd: RDD[(String, Array[Int])] = groupRdd.map { case (key: String, value: Iterable[Int]) => {
            //            val list: List[Int] = tuple._2.toList
            //
            //            val value: List[Int] = list.sorted.reverse.take(3)
            //            (tuple._1, value)

            //性能优化版
            //            val key: String = tuple._1
            //            val value: Iterable[Int] = tuple._2

            val number: Int = 3
            var top3 = new Array[Int](3)

            for (iter <- value) {

                import scala.util.control.Breaks._
                breakable {
                    for (i <- 0.until(number)) {
                        if (top3(i) == null) {
                            top3(i) = iter
                            break
                        } else if (iter > top3(i)) {
                            for (j <- (number - 1).to(i + 1, -1)) {
                                top3(j) = top3(j - 1)
                            }

                            top3(i) = iter
                            break
                        }
                    }
                }

            }

            (key, top3)
        }
        }

        resultRdd.foreach { tuple => {
            println(tuple._1)
            tuple._2.foreach(println(_))
            println("=========")
        }
        }

        spark.close()
    }
}
