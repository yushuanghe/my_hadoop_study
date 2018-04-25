package com.shuanghe.spark.sql.udaf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAFMain {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("StringUDF")
                .master("local")
                .getOrCreate()

        // 构造模拟数据
        val names = Array("Leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo")
        val namesRDD: RDD[String] = spark.sparkContext.parallelize(names, 5)
        val rowRDD: RDD[Row] = namesRDD.map(x => Row(x))
        val structType: StructType = StructType(Array(StructField("name", DataTypes.StringType, true)))
        val df: DataFrame = spark.createDataFrame(rowRDD, structType)

        df.createOrReplaceTempView("name")

        spark.udf.register("strCount", new StringCountUDAF)

        val resultDF: DataFrame = spark.sql("select name,strCount(name) from name group by name")

        resultDF.show()

        spark.close()
    }
}
