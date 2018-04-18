package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-19
 * Time: 上午12:23
 * To change this template use File | Settings | File Templates.
 * Description:JDBC数据源
 */
public class JDBCDataSourceJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JDBCDataSource")
                .master("local")
                .getOrCreate();

        //Map<String,String> options=new HashMap<>();
        //options.put("url","jdbc:mysql://shuanghe.com:3306/test");
        //options.put("dbtable","student_info");
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");

        Dataset<Row> infoDF = spark.read()
                //.options(options)
                .jdbc("jdbc:mysql://shuanghe.com:3306/test", "student_info", properties);
        infoDF.printSchema();
        infoDF.show();

        Dataset<Row> scoreDF = spark.read()
                .jdbc("jdbc:mysql://shuanghe.com:3306/test", "student_score", properties);
        scoreDF.printSchema();
        scoreDF.show();

        //可以运行
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = infoDF.javaRDD().mapToPair((Row row) ->
                new Tuple2<>(row.getString(0), row.getInt(1))
        ).join(scoreDF.javaRDD().mapToPair((Row row) ->
                new Tuple2<>(row.getString(0), row.getInt(1))));

        JavaRDD<Row> rowRDD = joinRDD.map((tuple) -> RowFactory.create(tuple._1, tuple._2._1, tuple._2._2))
                .filter((row) -> {
                    if (row.getInt(2) > 80) {
                        return true;
                    } else {
                        return false;
                    }
                });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> resultDF = spark.createDataFrame(rowRDD, structType);

        resultDF.printSchema();
        resultDF.show();

        resultDF.write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:mysql://shuanghe.com:3306/test", "good_student_info", properties);

        resultDF.foreachPartition((Iterator<Row> rows) -> {
            Connection conn = null;
            while (rows.hasNext()) {
                Row row = rows.next();
                //insert
            }
        });

        spark.close();
    }
}