package com.shuanghe.hadoop.shuffle;

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

/**
 * 二次排序
 * Created by yushuanghe on 2017/02/14.
 */
public class SecondarySortRunner {
    static class SecondarySortMapper extends Mapper<Object, Text, IntPair, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split("\t");
            if (strs.length == 2) {
                int first = Integer.valueOf(strs[0]);
                int second = Integer.valueOf(strs[1]);
                context.write(new IntPair(first, second), new IntWritable(second));
            } else {
                System.out.println("数据异常：" + line);
            }
        }
    }

    static class SecondarySortReducer extends Reducer<IntPair, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer preKey = key.getFirst();
            StringBuffer sb = new StringBuffer();

            for (IntWritable value : values) {
                int curKey = key.getFirst();
                if (preKey == curKey) {
                    //表示同一个key，但是value不一样或者是value是排序好的
                    sb.append(value.get() + ";");
                } else {
                    //表示新的一个key，先输出旧key对应的value信息,修改key值和stringbuffer值
                    context.write(new IntWritable(preKey), new Text(sb.toString()));
                    preKey = curKey;
                    sb = new StringBuffer();
                    sb.append(value.get() + ";");
                }
            }
            //输出最后的结果信息
            context.write(new IntWritable(preKey), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
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
                            job.setMapOutputValueClass(IntWritable.class);
                            job.setOutputKeyClass(IntWritable.class);
                            job.setOutputValueClass(Text.class);

                            //group
                            job.setGroupingComparatorClass(IntPairGrouping.class);

                            //partitioner
                            job.setPartitionerClass(IntPairPartitioner.class);
                            job.setNumReduceTasks(2);

                            //输入输出路径
                            FileInputFormat.addInputPath(job, new Path("shuffle/data.txt"));
                            FileOutputFormat.setOutputPath(job, new Path("shuffle/output" + System.currentTimeMillis()));

                            job.waitForCompletion(true);
                        } catch (IOException | InterruptedException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }
}
