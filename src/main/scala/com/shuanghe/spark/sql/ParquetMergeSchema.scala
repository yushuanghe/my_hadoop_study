package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ParquetMergeSchema {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("ParquetMergeSchema")
            .master("local")
            .getOrCreate

        val list = Array[Student](Student(1, "haha", 18)
            , Student(2, "dali", 19)
            , Student(3, "lili", 17))
        val rdd: RDD[Student] = spark.sparkContext.parallelize(list)
        val df: DataFrame = spark.createDataFrame(rdd)
        df.printSchema()
        df.write.mode(SaveMode.Overwrite)
            .parquet("spark/mergeSchema")

        val list2 = Array[(Int, Int)]((1, 90)
            , (2, 95)
            , (3, 98))
        val rdd2: RDD[(Int, Int)] = spark.sparkContext.parallelize(list2)
        val df2: DataFrame = spark.createDataFrame(rdd2)
        df2.printSchema()
        df2.write.mode(SaveMode.Append)
            .parquet("spark/mergeSchema")

        //df1和df2的元数据是不一样的
        val mergeDF: DataFrame = spark.read.option("mergeSchema", "true")
            .parquet("spark/mergeSchema")

        mergeDF.printSchema()
        mergeDF.show()

        spark.close()
    }
}
