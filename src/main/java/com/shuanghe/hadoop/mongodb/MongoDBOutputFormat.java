package com.shuanghe.hadoop.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 * 自定义mongodb OutputFormat
 * Created by yushuanghe on 2017/02/14.
 */
public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable, V> {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }

    /**
     * 自定义RecordWriter
     *
     * @param <V>
     */
    static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {

        private DBCollection dbCollection = null;

        public MongoDBRecordWriter() {
            super();
        }

        public MongoDBRecordWriter(TaskAttemptContext context) {
            Mongo mongo = new Mongo("127.0.0.1", 27017);
            DB db = mongo.getDB("hadoop");

            dbCollection = db.getCollection("result");

        }

        @Override
        public void write(NullWritable key, V value) throws IOException, InterruptedException {
            value.write(dbCollection);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }
}
