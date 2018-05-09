package com.shuanghe.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-9
 * Time: 下午11:17
 * To change this template use File | Settings | File Templates.
 * Description:与spark SQL 整合使用，top3热门商品实时统计
 */
public class Top3HotProductJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Top3HotProductJava")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        ssc.checkpoint("spark/streaming/checkpoint");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "shuanghe.com:9092");

        Set<String> topics = new HashSet<>();
        topics.add("test");

        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        //日志格式
        //username product category
        JavaDStream<String> productClickLogsDStream = messages.map((Function<Tuple2<String, String>, String>) (tuple) -> tuple._2);

        // 然后，应该是做一个映射，将每个种类的每个商品，映射为(category_product, 1)的这种格式
        // 从而在后面可以使用window操作，对窗口中的这种格式的数据，进行reduceByKey操作
        // 从而统计出来，一个窗口中的每个种类的每个商品的，点击次数
        JavaPairDStream<String, Integer> categoryProductPairsDStream = productClickLogsDStream.mapToPair((PairFunction<String, String, Integer>) (log) -> {
            String[] productClickLogSplited = log.split(" ");
            return new Tuple2<>(productClickLogSplited[2] + "_" + productClickLogSplited[1], 1);
        });

        // 然后执行window操作
        // 到这里，就可以做到，每隔10秒钟，对最近60秒的数据，执行reduceByKey操作
        // 计算出来这60秒内，每个种类的每个商品的点击次数
        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairsDStream.reduceByKeyAndWindow((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2
                , Durations.seconds(60), Durations.seconds(10));

        List<StructField> structFields = Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true)
                , DataTypes.createStructField("product", DataTypes.StringType, true)
                , DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Broadcast<StructType> broadcastStructType = sc.broadcast(structType);

        // 然后针对60秒内的每个种类的每个商品的点击次数
        // foreachRDD，在内部，使用Spark SQL执行top3热门商品的统计
        categoryProductCountsDStream.foreachRDD(
                (VoidFunction<JavaPairRDD<String, Integer>>) categoryProductCountsRDD -> {
                    // 将该RDD，转换为JavaRDD<Row>的格式
                    JavaRDD<Row> categoryProductCountRowRDD = categoryProductCountsRDD.map((Function<Tuple2<String, Integer>, Row>) (tuple) -> {
                                String category = tuple._1.split("_")[0];
                                String product = tuple._1.split("_")[1];
                                Integer count = tuple._2;
                                return RowFactory.create(category, product, count);
                            }
                    );

                    StructType structType1 = broadcastStructType.getValue();
                    Dataset<Row> categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDD, structType1);

                    categoryProductCountDF.createOrReplaceTempView("product_click_log");

                    // 执行SQL语句，针对临时表，统计出来每个种类下，点击次数排名前3的热门商品
                    Dataset<Row> top3ProductDF = spark.sql(
                            "SELECT category,product,click_count "
                                    + "FROM ("
                                    + "SELECT "
                                    + "category,"
                                    + "product,"
                                    + "click_count,"
                                    + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) AS rank "
                                    + "FROM product_click_log"
                                    + ") tmp "
                                    + "WHERE rank<=3");

                    // 这里说明一下，其实在企业场景中，可以不是打印的
                    // 按理说，应该将数据保存到redis缓存、或者是mysql db中
                    // 然后，应该配合一个J2EE系统，进行数据的展示和查询、图形报表
                    top3ProductDF.show();
                }
        );

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}