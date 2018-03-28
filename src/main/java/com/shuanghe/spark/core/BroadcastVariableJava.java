package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-28
 * Time: 下午11:08
 * To change this template use File | Settings | File Templates.
 * Description:广播变量
 */
public class BroadcastVariableJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariableJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        int factor = 3;

        // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
        // 获取的返回结果是Broadcast<T>类型
        Broadcast<Integer> broadcast = sc.broadcast(factor);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                // 使用共享变量时，调用其value()方法，即可获取其内部封装的值
                return v1 * broadcast.value();
            }
        }).foreach(x -> System.out.println(x));

        sc.close();
    }
}