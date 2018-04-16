package com.shuanghe.spark.sql

import com.shuanghe.hadoop.util.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

object ManuallySpecifyOptions {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("ManuallySpecifyOptions")
                .master("local")
                .getOrCreate()

        val df: DataFrame = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/people.json")

        HdfsUtil.deleteFile("spark/csvOutput", new Configuration())

        df.select("name").write
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv("spark/csvOutput")

        spark.close()
    }
}
