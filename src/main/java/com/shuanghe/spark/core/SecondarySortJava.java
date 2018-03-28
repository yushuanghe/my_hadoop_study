package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-29
 * Time: 上午12:22
 * To change this template use File | Settings | File Templates.
 * Description:二次排序
 * 1、需要实现自定义key，实现 Ordered<SecondarySortKey> 接口和scala 的 Serializable 接口
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 */
public class SecondarySortJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SecondarySortJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/sort.txt", 2);

        JavaPairRDD<SecondarySortKey, String> secondaryRdd = rdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {

            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] strs = s.split(" ");
                return new Tuple2<>(new SecondarySortKey(Integer.parseInt(strs[0]), Integer.parseInt(strs[1])), s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> secondarySortRdd = secondaryRdd.sortByKey();

        JavaRDD<String> resultRdd = secondarySortRdd.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        resultRdd.foreach(x -> System.out.println(x));

        sc.close();
    }
}