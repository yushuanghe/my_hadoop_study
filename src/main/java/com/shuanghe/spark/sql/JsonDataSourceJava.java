package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-17
 * Time: 下午11:12
 * To change this template use File | Settings | File Templates.
 * Description:json文件数据源与 JavaRDD<String> 类型作为json输入
 * 两张表join
 * 输出到json文件
 */
public class JsonDataSourceJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JsonDataSource")
                .master("local")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students2.json")
                .cache();

        df.createOrReplaceTempView("student_scores");

        Dataset<Row> goodDF = spark.sql("select name from student_scores where score >= 80");

        List<String> listName = goodDF.javaRDD()
                .map((Row row) -> row.getString(0))
                .collect();

        List<String> studentInfoJson = new ArrayList<>();
        studentInfoJson.add("{\"name\":\"Leo\",\"age\":18}");
        studentInfoJson.add("{\"name\":\"Marry\",\"age\":20}");
        studentInfoJson.add("{\"name\":\"Jack\",\"age\":16}");

        JavaRDD<String> rdd = sc.parallelize(studentInfoJson);

        //可以使用SQLContext.read.json()方法，针对一个元素类型为String的RDD，或者是一个JSON文件。
        //但是要注意的是，这里使用的JSON文件与传统意义上的JSON文件是不一样的。每行都必须，也只能包含一个，单独的，自包含的，有效的JSON对象。不能让一个JSON对象分散在多行。否则会报错。
        Dataset<Row> studentInfoDF = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json(rdd);

        studentInfoDF.createOrReplaceTempView("student_info");

        StringBuilder sb = new StringBuilder("select a.name,a.age,b.score from student_info a join student_scores b on a.name=b.name and a.name in (");

        for (String name : listName) {
            sb.append("'").append(name).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");

        Broadcast<String> broadcast = sc.broadcast(sb.toString());

        Dataset<Row> resultDF = spark.sql(broadcast.getValue());

        resultDF.show(false);

        JavaPairRDD<String, Integer> pairRDD = resultDF.javaRDD()
                .mapToPair((Row row) ->
                        new Tuple2<>(row.getString(0), (int) row.getLong(2))
                );

        //可以运行
        JavaPairRDD<String, Tuple2<Integer, Integer>> resultRdd = pairRDD.join(df.javaRDD().mapToPair((Row row) -> new Tuple2<>(row.getString(0), (int) row.getLong(1))
        ));

        resultRdd.foreach(x -> System.out.println(x));

        JavaRDD<Row> rowRDD = resultDF.javaRDD()
                .map((Row row) ->
                        RowFactory.create(row.getString(0), (int) row.getLong(1), (int) row.getLong(2))
                );

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> resultDf = spark.createDataFrame(rowRDD, structType);

        resultDf.printSchema();
        resultDf.show();

        resultDf.write()
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("spark/jsonOutput");

        spark.close();
    }
}