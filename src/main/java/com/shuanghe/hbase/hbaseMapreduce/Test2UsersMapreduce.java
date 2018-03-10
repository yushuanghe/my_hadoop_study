package com.shuanghe.hbase.hbaseMapreduce;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 下午8:21
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class Test2UsersMapreduce implements Tool {
    private Configuration conf;
    private static String family = "cf";
    private static String inTable = "test";
    private static String outTable = "users";

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new Test2UsersMapreduce(), args);

            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        // TODO: 18-3-9 mapreduce设置scan
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        // set other scan attrs
        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row2"));

        TableMapReduceUtil.initTableMapperJob(
                inTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                ReadTestMapper.class,     // mapper class
                Text.class,         // mapper output key
                Put.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                outTable,        // output table
                WriteUsersReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(1);   // at least one, adjust as required

        return job.waitForCompletion(true) ? 0 : -1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create();
        //idea中运行需要设置jar包
        this.conf.set("mapreduce.job.jar", "/home/yushuanghe/studyspace/my_hadoop_study/target/my_hadoop_study.jar");
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    static class ReadTestMapper extends TableMapper<Text, Put> {
        private Text outputKey = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //获取rowkey
            String rowkey = Bytes.toString(key.get());
            outputKey.set(rowkey);

            Put put = new Put(key.get());
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("id")
                    , value.getValue(Bytes.toBytes(family), Bytes.toBytes("id")));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name")
                    , value.getValue(Bytes.toBytes(family), Bytes.toBytes("name")));
            context.write(outputKey, put);
        }
    }

    static class WriteUsersReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        private ImmutableBytesWritable outputKey = null;

        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(outputKey, put);
            }
        }
    }
}