package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-28
 * Time: 下午11:30
 * To change this template use File | Settings | File Templates.
 * Description:累加器
 * spark1使用:Accumulator
 * spark2使用:AccumulatorV2<Long, Long>
 */
public class AccumulatorJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                //win临时跑需要参数
                .set("spark.driver.allowMultipleContexts", "true")
                .setAppName("AccumulatorJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkContext sparkContext = new SparkContext(conf);

        AccumulatorV2<Long, Long> accumulator = sparkContext.longAccumulator();

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                accumulator.add((long) v1);
                return v1;
            }
        });

        //rdd1 cache之后两次累加器结果一致
        rdd1
                //.cache()
                .count();
        System.out.println("accum1:" + accumulator.value());
        //rdd1没有被cache，两个count计算了两次，累加器累加两次
        rdd1.count();
        System.out.println("accum2:" + accumulator.value());
        //没有action，transformation是lazy的，实际不执行，累加器值为0

        sc.close();
    }
}