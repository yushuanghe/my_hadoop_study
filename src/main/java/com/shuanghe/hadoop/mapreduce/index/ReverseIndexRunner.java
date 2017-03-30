package com.shuanghe.hadoop.mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class ReverseIndexRunner {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            go();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void go() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
        Job job = Job.getInstance(conf, "reverse_index");

        FileInputFormat.addInputPath(job, new Path("mapreduce/input/word2.txt"));

        job.setJarByClass(ReverseIndexRunner.class);
        job.setMapperClass(ReverseIndexMapper.class);
        job.setReducerClass(ReverseIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("mapreduce/index/output"));

        job.waitForCompletion(true);
    }
}
