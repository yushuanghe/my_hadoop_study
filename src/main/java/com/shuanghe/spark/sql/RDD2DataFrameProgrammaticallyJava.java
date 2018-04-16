package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-16
 * Time: 下午10:20
 * To change this template use File | Settings | File Templates.
 * Description:以编程方式动态指定元数据，将RDD转换为DataFrame
 */
public class RDD2DataFrameProgrammaticallyJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RDD2DataFrameProgrammatically")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //创建一个 RDD<Row>
        JavaRDD<String> lines = sc.textFile("file:///home/yushuanghe/test/data/students.txt");

        JavaRDD<Row> rows = lines.map((line) -> {
            String[] strs = line.split(",");
            return RowFactory.create(Integer.parseInt(strs[0]), strs[1], Integer.parseInt(strs[2]));
        });

        //创建 schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fields);

        //将RDD转换为 DataFrame
        Dataset<Row> studentDF = spark.createDataFrame(rows, structType);

        studentDF.createOrReplaceTempView("student");

        Dataset<Row> teenagerDF = spark.sql("select id,name,age from student where age <= 18");

        JavaRDD<Row> teenagerRdd = teenagerDF.javaRDD();

        teenagerRdd.foreach(x -> System.out.println(x));

        spark.close();
    }
}