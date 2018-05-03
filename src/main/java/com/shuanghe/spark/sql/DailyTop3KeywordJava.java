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

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-1
 * Time: 下午4:57
 * To change this template use File | Settings | File Templates.
 * Description:每日top3热点搜索词统计案例
 */
public class DailyTop3KeywordJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DailyTop3KeywordJava")
                //.master("local")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //查询条件
        //备注：实际上，在实际的企业项目开发中，这个查询条件，是通过J2EE平台插入到某个mysql表中的
        //然后，通常是用spring和ORM框架（mybatis），提取mysql表中的查询条件
        Map<String, List<String>> rawQueryParamMap = new HashMap<>();
        rawQueryParamMap.put("city", Collections.singletonList("beijing"));
        rawQueryParamMap.put("platform", Collections.singletonList("android"));
        rawQueryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        //根据实现思路中的分析，这里最合适的方式，是将该查询条件map封装为一个 Broadcast 广播变量
        //这样可以进行优化，每个 Worker 节点，就拷贝一份数据即可
        final Broadcast<Map<String, List<String>>> queryParamBroadcast = sc.broadcast(rawQueryParamMap);

        JavaRDD<String> rawRDD = sc.textFile("spark/input/keyword.txt");

        //使用查询参数map广播变量，进行过滤
        JavaRDD<String> filterRDD = rawRDD.filter((String log) -> {
            String[] logSplited = log.split("\t");
            String city = logSplited[3];
            String platform = logSplited[4];
            String version = logSplited[5];

            //与查询条件进行比对
            Map<String, List<String>> queryParamMap = queryParamBroadcast.value();

            int score = 0;

            List<String> cities = queryParamMap.get("city");
            if (cities.size() > 0 && !cities.contains(city)) {
                score++;
            }
            List<String> platforms = queryParamMap.get("platform");
            if (platforms.size() > 0 && !platforms.contains(platform)) {
                score++;
            }
            List<String> versions = queryParamMap.get("version");
            if (versions.size() > 0 && !versions.contains(version)) {
                score++;
            }

            return score <= 0;
        });

        //将过滤出来的原始日志，映射为 (日期_搜索词，用户) 格式
        JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair((String log) -> {
            String[] logSplited = log.split("\t");
            String date = logSplited[0];
            String user = logSplited[1];
            String keyword = logSplited[2];

            return new Tuple2<>(date + "_" + keyword, user);
        });

        //进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();

        //对每天每个搜索词的搜索用户，执行去重操作，获得uv
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair((Tuple2<String, Iterable<String>> tuple) -> {
            String dateKeyword = tuple._1;

            Iterator<String> users = tuple._2.iterator();

            //对用户进行去重，并统计去重后的数量
            //Set<String> distinctUser = new HashSet<>();
            List<String> distinctUser = new ArrayList<>();
            while (users.hasNext()) {
                //distinctUser.add(users.next());
                String user = users.next();
                if (!distinctUser.contains(user)) {
                    distinctUser.add(user);
                }
            }
            long uv = distinctUser.size();

            return new Tuple2<>(dateKeyword, uv);
        });

        //将每天每个搜索词的uv数据，转换成 DataFrame
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map((Tuple2<String, Long> tuple) -> {
            String date = tuple._1.split("_")[0];
            String keyword = tuple._1.split("_")[1];
            Long uv = tuple._2;

            return RowFactory.create(date, keyword, uv);
        });

        List<StructField> structFields = Arrays
                .asList(DataTypes.createStructField("date", DataTypes.StringType, true)
                        , DataTypes.createStructField("keyword", DataTypes.StringType, true)
                        , DataTypes.createStructField("uv", DataTypes.LongType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dateKeywordUvDF = spark.createDataFrame(dateKeywordUvRowRDD, structType);

        //注册临时表
        dateKeywordUvDF.createOrReplaceTempView("date_keyword_uv");

        //使用 ROW_NUMBER 开窗函数，统计每天搜索uv排名前三的热点搜索词
        Dataset<Row> dailyTop3KeywordDF = spark.sql("select date,keyword,uv from (\n" +
                "select date,keyword,uv,row_number() over(partition by date order by uv desc) as rank\n" +
                "from date_keyword_uv\n" +
                ") t\n" +
                "where rank <= 3");

        //将 DataFrame 转换为 RDD ，然后映射，计算出每天的top3搜索词的搜索uv总数
        JavaRDD<Row> uvDateKeywordRDD = dailyTop3KeywordDF.javaRDD();

        JavaPairRDD<String, String> top3DateKeywordUvRDD = uvDateKeywordRDD.mapToPair((Row row) -> {
            String date = row.getString(0);
            String keyword = row.getString(1);
            Long uv = row.getLong(2);

            return new Tuple2<>(date, keyword + "_" + uv);
        });

        JavaPairRDD<String, Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();

        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordsRDD.mapToPair((Tuple2<String, Iterable<String>> tuple) -> {
            String date = tuple._1;
            Iterator<String> iterator = tuple._2.iterator();

            Long totalUv = 0L;
            StringBuilder sb = new StringBuilder(date);

            while (iterator.hasNext()) {
                String iter = iterator.next();
                //String keyword = iter.split("_")[0];
                Long uv = Long.valueOf(iter.split("_")[1]);
                totalUv += uv;
                //date;keyword_uv;keyword_uv;keyword_uv
                sb.append(";").append(iter);
            }

            return new Tuple2<>(totalUv, sb.toString());
        });

        //按照每天的总搜索uv进行倒序排序
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        //再次进行映射，将排序后的数据，映射回原始的格式，Iterable<Row>
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap((Tuple2<Long, String> tuple) -> {
            String dateKeywords = tuple._2;
            String[] strs = dateKeywords.split(";");
            String date = strs[0];//date
            List<Row> rows = new ArrayList<>();
            //top3，一共3组值
            rows.add(RowFactory.create(date
                    , strs[1].split("_")[0]//keyword
                    , Long.valueOf(strs[1].split("_")[1])));//uv
            rows.add(RowFactory.create(date
                    , strs[2].split("_")[0]
                    , Long.valueOf(strs[2].split("_")[1])));
            rows.add(RowFactory.create(date
                    , strs[3].split("_")[0]
                    , Long.valueOf(strs[3].split("_")[1])));

            return rows.iterator();
        });

        //将最终的结果转换为 DataFrame ，保存
        Dataset<Row> finalDF = spark.createDataFrame(sortedRowRDD, structType);

        //finalDF.write().saveAsTable("daily_top3_keyword_uv");
        finalDF
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("spark/daily_top3_keyword_uv");

        spark.close();
    }
}