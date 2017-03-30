package com.shuanghe.hadoop.mapreduce.oneMapReduceTop10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * 二次排序
 * Created by yushuanghe on 2017/02/14.
 */
public class SecondarySortRunner {
    static class SecondarySortMapper extends Mapper<Object, Text, IntPair, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(" ");
            for (String str : strs) {
                context.write(new IntPair(1, str), new Text(str));
            }
        }
    }

    static class SecondarySortReducer extends Reducer<IntPair, Text, IntWritable, Text> {

        private Map<String, Integer> topKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topKey = new HashMap<String, Integer>();
        }

        @Override
        protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String word = key.getSecond();
            int i = 0;
            for (Text t : values) {
                i++;
            }
            topKey.put(word, i);
            context.write(new IntWritable(i), new Text(word));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String key = "";
            int max = 0;
            for (Map.Entry<String, Integer> entry : topKey.entrySet()) {
                if (max < (int) entry.getValue()) {
                    System.err.println(entry.getKey() + ":" + entry.getValue());
                    max = (int) entry.getValue();
                    key = entry.getKey();
                }
            }
            context.write(new IntWritable(0), new Text("我是分割线"));
            context.write(new IntWritable(max), new Text(key));
        }
    }

    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        Configuration conf = new Configuration();
                        conf.set("fs.defaultFS", "hdfs://192.168.236.128");
                        try {
                            Job job = Job.getInstance(conf, "secondarySort");

                            job.setJarByClass(SecondarySortRunner.class);
                            job.setMapperClass(SecondarySortMapper.class);
                            job.setReducerClass(SecondarySortReducer.class);
                            job.setMapOutputKeyClass(IntPair.class);
                            job.setMapOutputValueClass(Text.class);
                            job.setOutputKeyClass(IntWritable.class);
                            job.setOutputValueClass(Text.class);

                            //group
                            job.setGroupingComparatorClass(IntPairGrouping.class);

                            //partitioner
                            job.setPartitionerClass(IntPairPartitioner.class);
                            job.setNumReduceTasks(1);

                            //输入输出路径
                            FileInputFormat.addInputPath(job, new Path("mapreduce/oneMapReduceTop10/sample2.txt"));
                            FileOutputFormat.setOutputPath(job, new Path("mapreduce/oneMapReduceTop10/output" + System.currentTimeMillis()));

                            job.waitForCompletion(true);
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
}
