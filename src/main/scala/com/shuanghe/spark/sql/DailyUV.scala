package com.shuanghe.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.apache.spark.sql.functions._

object DailyUV {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("DailyUV")
                .master("local")
                .getOrCreate()

        import spark.implicits._

        //构造用户访问日志数据
        val userAccessLog = Array("2015-10-01,1122"
            , "2015-10-01,1122"
            , "2015-10-01,1123"
            , "2015-10-01,1124"
            , "2015-10-01,1124"
            , "2015-10-02,1122"
            , "2015-10-02,1121"
            , "2015-10-02,1123"
            , "2015-10-02,1123")

        val userAccessLogRDD = spark.sparkContext.parallelize(userAccessLog, 5)
        val userAccessRowRDD: RDD[Row] = userAccessLogRDD.map { line => {
            val strs = line.split(",")
            Row(strs(0), strs(1).toInt)
        }
        }

        val structType = StructType(Array(StructField("date", DataTypes.StringType, true)
            , StructField("userId", DataTypes.IntegerType, true)))

        val userAccessLogRowDF: DataFrame = spark.createDataFrame(userAccessRowRDD, structType)

        //内置函数
        val resultDF: DataFrame = userAccessLogRowDF.groupBy("date")
                .agg(col("date").alias("date2"), countDistinct("userid").alias("uv"))
                .cache()

        resultDF.printSchema()
        resultDF.show()

        resultDF
                //要进行map操作，要先定义一个Encoder
                //这就增加了系统升级繁重的工作量了。为了更简单一些，幸运的dataset也提供了转化RDD的操作。因此只需要将之前dataframe.map
                //在中间修改为：dataframe.rdd.map即可
                .rdd.map { row => Row(row.getAs[String]("date"), row.getAs[Integer]("uv")) }
                .collect()
                .foreach(println)

        spark.close()
    }
}
