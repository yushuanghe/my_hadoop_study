package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-17
 * Time: 上午1:08
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class ParquetMergeSchemaJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetMergeSchema")
                .master("local")
                .getOrCreate();

        List<Student> list = new ArrayList<>();
        list.add(new Student(1, "haha", 18));
        list.add(new Student(2, "dali", 19));
        list.add(new Student(3, "lili", 17));
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Student> rdd = sc.parallelize(list);
        Dataset<Row> df = spark.createDataFrame(rdd, Student.class);
        df.printSchema();
        df.write().mode(SaveMode.Overwrite)
                .parquet("spark/mergeSchema");

        List<Score> list2 = new ArrayList<>();
        list2.add(new Score(1, 99));
        list2.add(new Score(2, 89));
        list2.add(new Score(3, 66));
        JavaRDD<Score> rdd2 = sc.parallelize(list2);
        Dataset<Row> df2 = spark.createDataFrame(rdd2, Score.class);
        df2.printSchema();
        df2.write().mode(SaveMode.Append)
                .parquet("spark/mergeSchema");

        Dataset<Row> mergeDF = spark.read().option("mergeSchema", "ture")
                .parquet("spark/mergeSchema");
// TODO: 18-4-17 JavaRDD 反射无法获取 schema 
        mergeDF.printSchema();
        mergeDF.show();

        spark.close();
    }
}

class Score {
    private int id;
    private int score;

    public Score(int id, int score) {
        this.id = id;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}