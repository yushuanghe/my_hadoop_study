package com.shuanghe.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object Top3HotProduct {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("Top3HotProduct")
                .master("local[2]")
                .getOrCreate()

        val sc: SparkContext = spark.sparkContext
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

        ssc.checkpoint("spark/streaming/checkpoint")

        val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> "shuanghe.com:9092")

        val topics: Set[String] = Set[String]("test")

        val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val productClickLogsDStream: DStream[String] = messages.map {
            tuple: (String, String) => tuple._2
        }

        val categoryProductPairsDStream: DStream[(String, Int)] = productClickLogsDStream.map {
            log: String => (log.split(" ")(2) + "_" + log.split(" ")(1), 1)
        }

        val categoryProductCountsDStream: DStream[(String, Int)] = categoryProductPairsDStream.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2
            , Seconds(60), Seconds(10))

        val structType: StructType = StructType(Seq[StructField](
            StructField("category", StringType, nullable = true)
            , StructField("product", StringType, nullable = true)
            , StructField("click_count", IntegerType, nullable = true)))

        val broadcast: Broadcast[StructType] = sc.broadcast[StructType](structType)

        categoryProductCountsDStream.foreachRDD {
            categoryProductCountsRDD: RDD[(String, Int)] => {
                val categoryProductCountRowRDD: RDD[Row] = categoryProductCountsRDD.map {
                    case (key: String, click_count: Int) => {
                        val category = key.split("_")(0)
                        val product = key.split("_")(1)

                        Row(category, product, click_count)
                    }
                }

                val structType1: StructType = broadcast.value
                val categoryProductCountDF: DataFrame = spark.createDataFrame(categoryProductCountRowRDD, structType1)

                categoryProductCountDF.createOrReplaceTempView("product_click_log")

                val top3ProductDF: DataFrame = spark.sql {
                    "select " +
                            "category,product,click_count " +
                            "from (" +
                            "select " +
                            "category,product,click_count," +
                            "row_number() over (partition by category order by click_count desc) as rank " +
                            "from product_click_log " +
                            ") t " +
                            "where rank <= 3"
                }

                top3ProductDF.show()
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
