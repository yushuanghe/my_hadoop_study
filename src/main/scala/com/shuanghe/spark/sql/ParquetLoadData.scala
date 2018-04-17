package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetLoadData {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("ParquetLoadData")
                .master("local")
                .getOrCreate()

        val df: DataFrame = spark.read.parquet("file:///home/yushuanghe/test/data/users.parquet")

        df.createOrReplaceTempView("user")

        df.show()

        val userDF: DataFrame = spark.sql("select favorite_color from user")

        val rdd: RDD[String] = userDF.rdd
                .map { row => {
                    "favorite_color:" + row.getAs[String]("favorite_color")
                }
                }

        rdd.foreach(println(_))

        spark.close()
    }
}
