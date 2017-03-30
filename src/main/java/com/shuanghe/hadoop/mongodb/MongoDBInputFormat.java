package com.shuanghe.hadoop.mongodb;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义MongoDB InputFormat
 * Created by yushuanghe on 2017/02/14.
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {
    /**
     * 获取分片信息集合
     *
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        //获取mongodb连接ei
        Mongo mongo = new Mongo("127.0.0.1", 27017);
        DB db = mongo.getDB("hadoop");
        //获取mongodb集合
        DBCollection dbCollection = db.getCollection("persons");

        //设置每2条数据一个mapper
        int chunkSize = 2;
        long size = dbCollection.count();//获取mongoDB 集合的条数
        long chunk = size / chunkSize;//计算mapper个数

        List<InputSplit> list = new ArrayList<InputSplit>();
        for (int i = 0; i < chunk; i++) {
            if (i == (chunk - 1)) {
                list.add(new MongoDBInputSplit(i * chunkSize, size));
            } else {
                list.add(new MongoDBInputSplit(i * chunkSize, i * chunkSize + chunkSize));
            }
        }

        mongo.close();

        return list;
    }

    /**
     * 获取RecordReader
     *
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<LongWritable, V>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new MongoDBRecordReader(split, context);
    }

    /**
     * MongoDB自定义InputSplit
     * 自定义分片信息类
     */
    static class MongoDBInputSplit extends InputSplit implements Writable {
        //[start,end)
        private long start;//起始位置，包含
        private long end;//结束位置，不包含

        public MongoDBInputSplit() {
            super();
        }

        public MongoDBInputSplit(long start, long end) {
            super();
            this.start = start;
            this.end = end;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return end - start;
        }

        /**
         * 返回长度为0的数组
         *
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.start);
            out.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.start = in.readLong();
            this.end = in.readLong();
        }
    }

    /**
     * 自定义mongodb RecordReader类
     */
    static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {

        private MongoDBInputSplit split;
        private DBCursor dbCursor;
        private LongWritable key;
        private V value;
        private int index;

        MongoDBRecordReader() {
            super();
        }

        MongoDBRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            this.initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.index = 0;
            this.split = (MongoDBInputSplit) split;
            Configuration conf = context.getConfiguration();
            this.key = new LongWritable();
            Class<?> clz = conf
                    .getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
            //使用发射工具类获取value对象
            value = (V) ReflectionUtils.newInstance(clz, conf);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (dbCursor == null) {
                Mongo mongo = new Mongo("127.0.0.1", 27017);
                DB db = mongo.getDB("hadoop");
                //获取mongodb集合
                DBCollection dbCollection = db.getCollection("persons");
                //获取DBCursor对象
                dbCursor = dbCollection.find().skip((int) this.split.start)
                        .limit((int) this.split.getLength());
            }

            boolean hasNext = this.dbCursor.hasNext();
            if (hasNext) {
                /**
                 * key是位置信息，value是读取到的数据
                 */
                DBObject dbObject = this.dbCursor.next();
                this.key.set(this.split.start + index);
                this.index++;

                this.value.readFields(dbObject);
            }

            return hasNext;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }

        /**
         * 获取操作进度信息，可以直接返回0
         *
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            this.dbCursor.close();
        }
    }

    /**
     * 一个空的mongodb自定义数据类型
     */
    static class NullMongoDBWritable implements MongoDBWritable {

        @Override
        public void readFields(DBObject dbObject) {

        }

        @Override
        public void write(DBCollection dbCollection) {

        }

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

        }
    }
}
