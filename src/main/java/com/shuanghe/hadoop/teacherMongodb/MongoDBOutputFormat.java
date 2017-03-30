package com.shuanghe.hadoop.teacherMongodb;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

/**
 * 自定义outputformat
 *
 * @param <V>
 * @author gerry
 */
public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable, V> {

    /**
     * 自定义mongodb outputformat
     *
     * @param <V>
     * @author gerry
     */
    static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {
        private DBCollection dbCollection = null;

        public MongoDBRecordWriter() {
        }

        public MongoDBRecordWriter(TaskAttemptContext context) throws IOException {
            // 获取mongo连接
            Mongo mongo = new Mongo("127.0.0.1", 27017);
            DB db = mongo.getDB("hadoop");
//        DB mongo = Mongo.connect(new DBAddress("127.0.0.1", "hadoop"));
            // 获取mongo集合
            dbCollection = db.getCollection("result");
        }

        @Override
        public void write(NullWritable key, V value) throws IOException, InterruptedException {
            value.write(this.dbCollection);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 没有关闭方法
        }
    }

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new MongoDBRecordWriter<>(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }

}
