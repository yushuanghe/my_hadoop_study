package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-29
 * Time: 上午12:57
 * To change this template use File | Settings | File Templates.
 * Description:分组top3
 *
 * @author yusuhanghe
 */
public class GroupTop3Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("GroupTop3Java")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/score.txt", 2);

        JavaPairRDD<String, Integer> pairRdd = rdd.mapToPair((PairFunction<String, String, Integer>) s -> {
            String[] strs = s.split(" ");
            return new Tuple2<>(strs[0], Integer.parseInt(strs[1]));
        });

        JavaPairRDD<String, Iterable<Integer>> groupRdd = pairRdd.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> groupTopRdd = groupRdd.mapToPair((PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>) tuple -> {
            String key = tuple._1;
            Iterable<Integer> list = tuple._2;
            //List<Integer> list = new ArrayList<>();
            //
            //for (Integer a_2 : tuple._2) {
            //    list.add(a_2);
            //}
            //
            //list.sort((a, b) -> b - a);
            //
            //List<Integer> result = Arrays.asList(list.get(0), list.get(1), list.get(2));
            //
            //return new Tuple2<>(tuple._1, result);

            //性能优化版
            //要取的topn数量
            int number = 3;
            Integer[] top3 = new Integer[number];

            for (int iter : list) {

                for (int i = 0; i < number; i++) {
                    if (top3[i] == null) {
                        top3[i] = iter;
                        break;
                    } else if (iter > top3[i]) {
                        for (int j = number - 1; j > i; j--) {
                            top3[j] = top3[j - 1];
                        }

                        top3[i] = iter;
                        break;
                    }
                }

            }

            return new Tuple2<>(key, Arrays.asList(top3));
        });

        groupTopRdd.foreach(x -> System.out.println(x));

        sc.close();
    }
}