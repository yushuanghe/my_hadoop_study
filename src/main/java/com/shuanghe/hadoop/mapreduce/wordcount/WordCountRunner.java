package com.shuanghe.hadoop.mapreduce.wordcount;

import com.shuanghe.hadoop.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedAction;

/**
 * Created by yushuanghe on 2017/02/13.
 */
public class WordCountRunner implements Tool {
    private Configuration conf;

    public static void main(String[] args) {
        final String[] args2 = args;
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            ToolRunner.run(new WordCountRunner(), args2);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCountRunner.class);

        //输入
        FileInputFormat.addInputPath(job, new Path("mapreduce/input/word2.txt"));

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle

        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //output
        HdfsUtil.deleteFile("mapreduce/output");
        FileOutputFormat.setOutputPath(job, new Path("mapreduce/output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
