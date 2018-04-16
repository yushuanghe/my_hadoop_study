package com.shuanghe.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveModeTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("SaveModeTest")
                .master("local")
                .getOrCreate()

        val df = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/people.json")

        df.write
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv("spark/csvOutput")

        spark.close()
    }
}
