package com.shuanghe.hadoop.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.io.Writable;

/**
 * mongoDB自定义数据类型
 * Created by yushuanghe on 2017/02/14.
 */
public interface MongoDBWritable extends Writable {
    /**
     * 从mongodb中读取数据
     *
     * @param dbObject
     */
    public void readFields(DBObject dbObject);

    /**
     * 往mongodb中写入数据
     *
     * @param dbCollection
     */
    public void write(DBCollection dbCollection);

}
