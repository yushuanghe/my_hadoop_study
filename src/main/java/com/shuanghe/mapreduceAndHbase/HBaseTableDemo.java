package com.shuanghe.mapreduceAndHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;

/**
 * mapreduce操作HBase
 * Created by yushuanghe on 2017/02/16.
 */
public class HBaseTableDemo {
    static class DemoMapper extends TableMapper<Text, ProductModel> {

        private Text outputKey;
        private ProductModel outputValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = new Text();
            this.outputValue = new ProductModel();
        }

        /**
         * key是rowkey，value是result
         * Result result = hTable.get(get);
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
            if (content == null) {
                System.out.println("数据格式错误" + content);
                return;
            }

            Map<String, String> map = HBaseTableOnlyMapperDemo.transfoerContent2Map(content);
            if (map.containsKey("p_id")) {
                //存在outputKey
                outputKey.set(map.get("p_id"));
            } else {
                System.out.println("数据格式错误" + content);
                return;
            }

            if (map.containsKey("p_name") && map.containsKey("price")) {
                //数据正常，进行赋值
                outputValue.setId(outputKey.toString());
                outputValue.setName(map.get("p_name"));
                outputValue.setPrice(map.get("price"));
            } else {
                System.out.println("数据格式错误" + content);
                return;
            }

            context.write(outputKey, outputValue);
        }
    }

    /**
     * 往HBase输出
     */
    static class DemoReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<ProductModel> values, Context context) throws IOException, InterruptedException {
            for (ProductModel value : values) {
                //如果有多个p_id，只拿一个
                ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));

                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
                context.write(outputKey, put);
            }
        }
    }

    /**
     * 执行入口
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                (PrivilegedAction<Object>) () -> {
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("fs.defaultFS", "hdfs://hadoop.shuanghe.com:8020");
                    conf.set("hbase.zookeeper.quorum", "hadoop.shuanghe.com:2181");

                    Job job = null;
                    try {
                        job = Job.getInstance(conf, "mapreduceAndHbase");
                        job.setJarByClass(HBaseTableDemo.class);

                        //设置mapper相关
                        //本地运行
//                            TableMapReduceUtil.initTableMapperJob("data", new Scan(),
//                                    DemoMapper.class, Text.class, ProductModel.class, job, false);
                        //集群运行
                        TableMapReduceUtil.initTableMapperJob("data", new Scan(),
                                DemoMapper.class, Text.class, ProductModel.class, job);

                        //设置reducer相关
                        //本地运行
//                            TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class,
//                                    job, null, null, null, null, false);
                        //集群运行
                        TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class,
                                job);

                        String exitCode = job.waitForCompletion(true) ? "执行成功！" : "执行失败！";
                        System.out.println(exitCode);
                    } catch (IOException | InterruptedException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    return null;
                }
        );

    }
}
