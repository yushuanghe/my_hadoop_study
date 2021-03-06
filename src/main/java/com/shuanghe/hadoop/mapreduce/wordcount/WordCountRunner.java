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
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
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
        FileInputFormat.addInputPath(job, new Path("mapreduce/input/word.txt"));

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //combiner
        job.setCombinerClass(WordCountReducer.class);

        //shuffle

        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //output
        HdfsUtil.deleteFile("mapreduce/output", conf);
        FileOutputFormat.setOutputPath(job, new Path("mapreduce/output"));

        return job.waitForCompletion(true) ? 0 : 1;
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
}
