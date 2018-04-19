package com.shuanghe.spark.sql

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object DailySale {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("DailySale")
                .master("local")
                .getOrCreate()

        import spark.implicits._

        val userSaleLog = Array("2015-10-01,55.05,1122"
            , "2015-10-01,23.15,1133"
            , "2015-10-01,15.20,"
            , "2015-10-02,56.05,1144"
            , "2015-10-02,78.87,1155"
            , "2015-10-02,113.02,1123")

        val userSaleLogRDD = spark.sparkContext.parallelize(userSaleLog, 5)
        val userSaleRowRDD = userSaleLogRDD
                .filter(line => line.split(",").length == 3)
                .map { line => {
                    val strs = line.split(",")
                    Row(strs(0), strs(1).toDouble)
                }
                }

        val structType = StructType(Array(StructField("date", DataTypes.StringType, true)
            , StructField("sale", DataTypes.DoubleType, true)
        ))

        val userSaleLogDF: DataFrame = spark.createDataFrame(userSaleRowRDD, structType)

        val resultDF: DataFrame = userSaleLogDF.groupBy("date")
                .agg(col("date").alias("date2"), sum(col("sale")).alias("sum"))
                .cache()

        resultDF.printSchema()
        resultDF.show()

        resultDF.rdd.map(row => Row(row.getString(0), row.getDouble(2)))
                .collect()
                .foreach(println)

        spark.close()
    }
}
