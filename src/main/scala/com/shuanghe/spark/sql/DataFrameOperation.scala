package com.shuanghe.spark.sql

import org.apache.spark.sql.SparkSession

object DataFrameOperation {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("DataFrameOperation")
                .master("local")
                .getOrCreate()

        val df = spark.read
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students.json")

        df.show()
        df.printSchema()
        df.select("name").show()
        df.select(df.col("name"), df.col("age").plus(100)).show()
        df.filter(df.col("age").gt(18)).show()
        df.groupBy(df.col("age")).count().show()

        spark.close()
    }
}
