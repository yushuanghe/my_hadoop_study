package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DataFrameProgrammatically {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("RDD2DataFrameProgrammatically")
                .master("local")
                .getOrCreate()

        val rdd: RDD[Row] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/students.txt")
                .map(line => {
                    val strs: Array[String] = line.split(",")
                    Row(strs(0).toInt, strs(1), strs(2).toInt)
                })

        val structType: StructType = StructType(Array(StructField("id", IntegerType, true)
            , StructField("name", StringType, true)
            , StructField("age", IntegerType, true)))

        val studentDF: DataFrame = spark.createDataFrame(rdd, structType)

        studentDF.createOrReplaceTempView("student")

        val teenagerDF: DataFrame = spark.sql("select id,name,age from student where age <= 18")

        teenagerDF.rdd.foreach(println(_))

        spark.close()
    }
}
