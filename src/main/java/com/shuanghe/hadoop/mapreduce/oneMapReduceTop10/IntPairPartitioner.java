package com.shuanghe.hadoop.mapreduce.oneMapReduceTop10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class IntPairPartitioner extends Partitioner<IntPair, Text> {

    /**
     * 返回值是0到（numPartitions-1）
     *
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(IntPair key, Text value, int numPartitions) {
        if (numPartitions > 1) {
            int first = key.getFirst();
            if (first % 2 == 0) {
                return 1;
            } else {
                //在第一个reducer处理
                return 0;
            }
        } else {
//            throw new IllegalArgumentException("reducer个数必须大于1");
            return 0;
        }
    }
}
