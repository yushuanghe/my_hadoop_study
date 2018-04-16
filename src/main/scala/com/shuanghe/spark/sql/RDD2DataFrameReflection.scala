package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class Student(id: Int, name: String, age: Int)

/**
  * 通过反射的方式，将RDD转换为 DataFrame
  */
object RDD2DataFrameReflection {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("RDD2DataFrameReflection")
                .master("local")
                .getOrCreate()

        val rdd: RDD[Student] = spark.sparkContext.textFile("file:///home/yushuanghe/test/data/students.txt")
                .map { x => {
                    val strs: Array[String] = x.split(",")
                    Student(strs(0).toInt, strs(1), strs(2).toInt)
                }
                }
        //                        .toDF()

        //反射方式创建 DataFrame
        val df: DataFrame = spark.createDataFrame(rdd)

        df.printSchema()

        df.createOrReplaceTempView("student")

        val teenagerDf: Dataset[Row] = spark.sql("select id,name,age from student where age <= 18")

        val teenagerStudentRDD: RDD[Student] = teenagerDf.rdd
                .map {
                    row: Row => {
                        Student(row.getInt(0), row.getString(1), row.getInt(2))
                    }
                }

        teenagerStudentRDD.foreach(println(_))

        // row.getAs[Int]("id") 获取指定列
        val teenagerStudentRDD2: RDD[Student] = teenagerDf.rdd
                .map { row: Row => {
                    Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age"))
                }
                }

        teenagerStudentRDD2.foreach(println(_))

        // row.getValuesMap[Any](List("name", "age")) 返回Map
        teenagerDf.rdd
                .map { row: Row => {
                    row.getValuesMap[Any](List("name", "age"))
                }
                }
                .foreach(println(_))

        spark.close()
    }
}
