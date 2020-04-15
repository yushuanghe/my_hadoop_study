package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object JsonDataSource {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("JsonDataSource")
                .master("local")
                .getOrCreate()

        val studentScoreDF: DataFrame = spark.read
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students2.json")

        studentScoreDF.createOrReplaceTempView("student_scores")

        studentScoreDF.show()

        val goodStudentScoreDF: DataFrame = spark.sql("select name from student_scores where score >= 80")

        val nameList: Array[String] = goodStudentScoreDF.rdd
                .map(row => row.getAs[String]("name")).collect()

        val studentInfoJson: Array[String] = Array[String]("{\"name\":\"Leo\",\"age\":18}"
            , "{\"name\":\"Marry\",\"age\":20}"
            , "{\"name\":\"Jack\",\"age\":16}")
        val studentInfoJsonRDD: RDD[String] = spark.sparkContext.parallelize(studentInfoJson)
        val studentInfoDF: DataFrame = spark.read
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json(studentInfoJsonRDD)

        studentInfoDF.createOrReplaceTempView("student_info")

        var sql = "select name,age from student_info where name in ("
        for (s <- nameList.indices) {
            if (s < (nameList.length - 1)) {
                sql += "'" + nameList(s) + "',"
            } else {
                sql += "'" + nameList(s) + "')"
            }
        }

        println(sql)

        val goodStudentInfoDF: DataFrame = spark.sql(sql)

        goodStudentInfoDF.show()

        val joinRDD: RDD[(String, (Int, Int))] = goodStudentInfoDF.rdd.map { row => {
            (row.getAs[String]("name"), row.getAs[Long]("age").toInt)
        }
        }.join {
            studentScoreDF.rdd.map { row => {
                (row.getAs[String]("name"), row.getAs[Long]("score").toInt)
            }
            }
        }

        val resultRDD: RDD[Row] = joinRDD.map { tuple => {
            Row(tuple._1, tuple._2._1, tuple._2._2)
        }
        }

        val structType: StructType = StructType(Seq(StructField("name", DataTypes.StringType, true)
            , StructField("age", DataTypes.IntegerType, true)
            , StructField("score", DataTypes.IntegerType, true)))

        val resultDF: DataFrame = spark.createDataFrame(resultRDD, structType)

        resultDF.printSchema()
        resultDF.show(false)

        resultDF.write
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("spark/jsonOutput")

        spark.close()
    }
}
