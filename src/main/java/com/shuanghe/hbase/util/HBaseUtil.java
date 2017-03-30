package com.shuanghe.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by yushuanghe on 2017/02/16.
 */
public class HBaseUtil {
    /**
     * 获取HBase配置文件信息
     *
     * @return
     */
    public static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop.shuanghe.com:2181");
        return conf;
    }
}
