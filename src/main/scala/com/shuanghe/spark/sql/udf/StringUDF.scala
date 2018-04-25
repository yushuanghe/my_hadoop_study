package com.shuanghe.spark.sql.udf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object StringUDF {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("StringUDF")
                .master("local")
                .getOrCreate()

        // 构造模拟数据
        val names = Array("Leo", "Marry", "Jack", "Tom")
        val namesRDD: RDD[String] = spark.sparkContext.parallelize(names, 5)
        val rowRDD: RDD[Row] = namesRDD.map(x => Row(x))
        val structType: StructType = StructType(Array(StructField("name", DataTypes.StringType, true)))
        val df: DataFrame = spark.createDataFrame(rowRDD, structType)

        df.createOrReplaceTempView("name")

        // 定义和注册自定义函数
        // 定义函数：自己写匿名函数
        // 注册函数：SQLContext.udf.register()
        spark.udf.register("strLen", (str: String) => str.length)

        val resultDF: DataFrame = spark.sql("select name,strLen(name) from name")

        resultDF.show()

        spark.close()
    }
}
