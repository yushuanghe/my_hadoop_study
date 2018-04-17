package com.shuanghe.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * 读取各种类型数据
  */
object ReadDataTypes {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("ReadDataTypes")
                .master("local")
                .getOrCreate()

        //        readCsv(spark)
        readTsv(spark)

        spark.close()
    }

    def readCsv(spark: SparkSession): Unit = {
        val data = spark.read
                .option("inferSchema", true) //推断数据类型
                .option("nullValue", "?") //设置空值
                .option("header", true) //是否有表头
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv("file:///home/yushuanghe/test/data/block_10.csv")
                .cache()

        data.printSchema()
        data.show(false) //不把数据截断
    }

    //自定义表头，读取tsv文件
    def readTsv(spark: SparkSession): Unit = {
        val cols = Seq("user_id", "item_id", "rating", "timestamp")

        val data = spark.read
                .option("inferSchema", true) //推断数据类型
                .option("header", false) //无表头
                .option("delimiter", "\t") //分隔符
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv("file:///home/yushuanghe/test/data/block_10.tsv")
                .toDF(cols: _*)

        data.printSchema()
        data.show(false)
    }

    def readJson(spark: SparkSession): Unit = {
        //按顺序把类型全写下来
        val schema: StructType = StructType(Seq(
            StructField("tid", IntegerType, true),
            StructField("uid", IntegerType, true),
            StructField("st", TimestampType, true),
            StructField("td", StringType, true),
            StructField("trail", ArrayType(StructType(Seq(
                StructField("id", IntegerType, true),
                StructField("ts", LongType, true),
                StructField("lat", DoubleType, true),
                StructField("alt", DoubleType, true),
                StructField("lon", DoubleType, true),
                StructField("d", StringType, true)))), true)))

        val data = spark.read
                .schema(schema)
                .json("jsonPath")
                .cache()

        data.printSchema()
        data.show(1, false)
    }
}
