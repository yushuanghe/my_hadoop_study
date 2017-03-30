package com.shuanghe.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHTableConnectionPool {
    public static void main(String[] args) {

    }

    /**
     * HBase线程池
     *
     * @param conf
     * @throws IOException
     */
    static void testUseHBaseConnectionPool(Configuration conf) throws IOException {
        ExecutorService threads = Executors.newFixedThreadPool(2);
        HConnection pool = HConnectionManager.createConnection(conf);
        HTableInterface hTableInterface = pool.getTable("users");
    }
}
