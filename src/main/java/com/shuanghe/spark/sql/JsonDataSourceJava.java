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

        JavaRDD<String> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(studentInfoJson);

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

        Broadcast<String> broadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(sb.toString());

        Dataset<Row> resultDF = spark.sql(broadcast.getValue());

        resultDF.show(false);

        JavaPairRDD<String, Integer> pairRDD = resultDF.javaRDD()
                .mapToPair((Row row) -> {
                    return new Tuple2<String, Integer>(row.getString(0), Integer.parseInt(String.valueOf(row.getLong(2))));
                });

        //JavaPairRDD<String, Tuple2<Integer, Integer>> resultRdd = pairRDD.join(df.javaRDD().mapToPair((Row row) -> new Tuple2<String, Integer>(row.getString(0), Integer.parseInt(String.valueOf(row.getLong(1))))
        //));

        JavaRDD<Row> rowRDD = resultDF.javaRDD()
                .map((Row row) -> {
                    return RowFactory.create(row.getString(0), (int) row.getLong(1), (int) row.getLong(2));
                });

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