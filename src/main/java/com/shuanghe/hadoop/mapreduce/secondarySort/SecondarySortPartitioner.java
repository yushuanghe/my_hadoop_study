package com.shuanghe.hadoop.mapreduce.secondarySort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class SecondarySortPartitioner extends Partitioner<SecondarySortPair, IntWritable> {

    /**
     * 返回值是0到（numPartitions-1）
     *
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(SecondarySortPair key, IntWritable value, int numPartitions) {
        if (numPartitions > 1) {
            String first = key.getFirst();
            if (StringUtils.isNotBlank(first)) {
                if (first.length() % 2 == 0) {
                    return 1;
                } else {
                    //在第一个reducer处理
                    return 0;
                }
            } else {
                return 0;
            }
        } else {
//            throw new IllegalArgumentException("reducer个数必须大于1");
            return 0;
        }
    }
}
