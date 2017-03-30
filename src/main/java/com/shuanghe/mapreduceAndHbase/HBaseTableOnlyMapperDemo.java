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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * mapreduce操作HBase
 * 直接使用mapper进行hbase输出
 * Created by yushuanghe on 2017/02/16.
 */
public class HBaseTableOnlyMapperDemo {
    static class DemoMapper extends TableMapper<ImmutableBytesWritable, Put> {

        private ImmutableBytesWritable outputKey;
        private Put outputValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = new ImmutableBytesWritable();
        }

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
                outputKey = new ImmutableBytesWritable(Bytes.toBytes(map.get("p_id")));
                outputValue = new Put(Bytes.toBytes(map.get("p_id")));
            } else {
                System.out.println("数据格式错误" + content);
                return;
            }

            if (map.containsKey("p_name") && map.containsKey("price")) {
                //数据正常，进行赋值
                outputValue.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(map.get("p_id")));
                outputValue.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(map.get("p_name")));
                outputValue.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(map.get("price")));
            } else {
                System.out.println("数据格式错误" + content);
                return;
            }

            context.write(outputKey, outputValue);
        }
    }

    /**
     * 执行入口
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        Configuration conf = HBaseConfiguration.create();
                        conf.set("fs.defaultFS", "hdfs://hadoop.shuanghe.com:8020");
                        conf.set("hbase.zookeeper.quorum", "hadoop.shuanghe.com:2181");

                        Job job = null;
                        try {
                            job = Job.getInstance(conf, "mapreduceAndHbase");
                            job.setJarByClass(HBaseTableOnlyMapperDemo.class);

                            //设置mapper相关
                            //本地运行
                            TableMapReduceUtil.initTableMapperJob("data", new Scan(),
                                    DemoMapper.class, ImmutableBytesWritable.class, Put.class, job, false);
                            //集群运行
//                            TableMapReduceUtil.initTableMapperJob("data", new Scan(),
//                                    DemoMapper.class, Text.class, ProductModel.class, job);

                            //设置reducer相关
                            //本地运行
                            TableMapReduceUtil.initTableReducerJob("online_product", null,
                                    job, null, null, null, null, false);
                            //集群运行
//                            TableMapReduceUtil.initTableReducerJob("online_product", null,
//                                    job);

                            job.setNumReduceTasks(0);

                            String exitCode = job.waitForCompletion(true) ? "执行成功！" : "执行失败！";
                            System.out.println(exitCode);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );

    }

    /**
     * 转换content为map对象
     *
     * @param content
     * @return
     */
    static Map<String, String> transfoerContent2Map(String content) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (tokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                //当前值为value
                map.put(key, tokenizer.nextToken());
            } else {
                //当前值为key
                key = tokenizer.nextToken();
            }
        }
        return map;
    }
}
