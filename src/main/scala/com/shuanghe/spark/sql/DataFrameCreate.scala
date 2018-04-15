package com.shuanghe.spark.sql

import org.apache.spark.sql.SparkSession

object DataFrameCreate {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("DataFrameCreate")
                .master("local")
                .getOrCreate()

        val df = spark.read
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students.json")

        df.printSchema()
        df.show()

        spark.close()
    }
}
