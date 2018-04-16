package com.shuanghe.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object GenericLoadSave {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("GenericLoadSave")
                .master("local")
                .getOrCreate()

        val df: DataFrame = spark.read.parquet("file:///home/yushuanghe/test/data/users.parquet")

        df.printSchema()

        df.show()

        df.select("name", "favorite_numbers")
                .write
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("spark/jsonOutput")

        spark.close()
    }
}
