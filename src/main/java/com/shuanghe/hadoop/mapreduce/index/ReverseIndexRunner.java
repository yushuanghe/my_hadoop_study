package com.shuanghe.hadoop.mapreduce.index;

import com.shuanghe.hadoop.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedAction;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class ReverseIndexRunner implements Tool {
    private Configuration conf;

    public static void main(String[] args) {
        final String[] args2 = args;
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            ToolRunner.run(new ReverseIndexRunner(), args2);
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

        Job job = Job.getInstance(conf, "reverse_index");

        FileInputFormat.addInputPath(job, new Path("mapreduce/input/word.txt"));

        job.setJarByClass(ReverseIndexRunner.class);
        job.setMapperClass(ReverseIndexMapper.class);

        job.setReducerClass(ReverseIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String outPathStr = "mapreduce/index/output";
        HdfsUtil.deleteFile(outPathStr, conf);
        FileOutputFormat.setOutputPath(job, new Path(outPathStr));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    @Override
    public void setConf(Configuration conf) {
        //conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
        conf.set("mapreduce.job.jar", "/home/yushuanghe/studyspace/my_hadoop_study/target/my_hadoop_study.jar");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
