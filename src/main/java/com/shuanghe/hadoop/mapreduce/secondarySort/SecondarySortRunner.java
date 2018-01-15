package com.shuanghe.hadoop.mapreduce.secondarySort;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * 二次排序
 * Created by yushuanghe on 2017/02/14.
 */
public class SecondarySortRunner implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "secondarySort");

        job.setJarByClass(SecondarySortRunner.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(SecondarySortPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(SecondarySortPair.class);
        job.setOutputValueClass(IntWritable.class);

        //partitioner
        job.setPartitionerClass(SecondarySortPartitioner.class);

        //group
        job.setGroupingComparatorClass(SecondarySortGrouping.class);

        job.setNumReduceTasks(1);

        //输入输出路径
        FileInputFormat.addInputPath(job, new Path("mapreduce/oneMapReduceTop10/word.txt"));

        FileOutputFormat.setOutputPath(job, new Path("mapreduce/oneMapReduceTop10/output" + System.currentTimeMillis()));

       return job.waitForCompletion(true)?0:-1;
    }

    @Override
    public void setConf(Configuration conf) {
//conf.set("fs.defaultFS", "hdfs://shuanghe.com:8020");
        //conf.set("yarn.resourcemanager.hostname", "shuanghe.com");
        //idea中运行需要设置jar包
        conf.set("mapreduce.job.jar", "/home/yushuanghe/studyspace/my_hadoop_study/target/my_hadoop_study.jar");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    static class SecondarySortMapper extends Mapper<Object, Text, SecondarySortPair, IntWritable> {
        private IntWritable outputValue=new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(" ");
            for (String str : strs) {
                context.write(new SecondarySortPair(str,(int)(Math.random()*10)), outputValue);
            }
        }
    }

    static class SecondarySortReducer extends Reducer<SecondarySortPair, IntWritable, SecondarySortPair, IntWritable> {

        private Map<String, Integer> topKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topKey = new HashMap<String, Integer>();
        }

        @Override
        protected void reduce(SecondarySortPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String word = key.getFirst();
            int serial=key.getSecond();
            int i = 0;
            for (IntWritable t : values) {
                i++;
            }
            //topKey.put(word, i);
            context.write(key, new IntWritable(i));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //String key = "";
            //int max = 0;
            //for (Map.Entry<String, Integer> entry : topKey.entrySet()) {
            //    if (max < (int) entry.getValue()) {
            //        System.err.println(entry.getKey() + ":" + entry.getValue());
            //        max = (int) entry.getValue();
            //        key = entry.getKey();
            //    }
            //}
            //context.write(new IntWritable(0), new Text("我是分割线"));
            //context.write(new IntWritable(max), new Text(key));
        }
    }

    public static void main(String[] args) {
        final String[] args2=args;
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            ToolRunner.run(new SecondarySortRunner(),args2);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }
}
