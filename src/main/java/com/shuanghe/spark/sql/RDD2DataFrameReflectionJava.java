package com.shuanghe.spark.sql;

import com.shuanghe.spark.sql.model.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-15
 * Time: 下午10:36
 * To change this template use File | Settings | File Templates.
 * Description:使用反射的方式将RDD转换为DataFrame
 */
public class RDD2DataFrameReflectionJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("RDD2DataFrameReflection")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<Student> rdd = sc.textFile("file:///home/yushuanghe/test/data/students.txt")
                .map((line) -> {
                    String[] strs = line.split(",");
                    return new Student(Integer.parseInt(strs[0]), strs[1], Integer.parseInt(strs[2]));
                });

        //使用反射方式，将RDD转换为DataFrame
        //将Student.class传进去，其实就是用反射的方式来创建DataFrame
        Dataset<Row> df = spark.createDataFrame(rdd, Student.class);

        df.printSchema();

        //df.registerTempTable("student");
        //2.0更新api
        df.createOrReplaceTempView("student");

        //针对 student 临时表执行sql
        Dataset<Row> teenagerDf = spark.sql("select id,name,age from student where age <= 18");

        JavaRDD<Row> teenagerRDD = teenagerDf.javaRDD();

        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map((Row row) ->
                new Student(row.getInt(0), row.getString(1), row.getInt(2))
        );

        teenagerStudentRDD.foreach(s -> System.out.println(s));

        spark.close();
    }
}