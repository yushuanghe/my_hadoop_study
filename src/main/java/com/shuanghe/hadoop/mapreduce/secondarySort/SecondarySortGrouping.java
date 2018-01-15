package com.shuanghe.hadoop.mapreduce.secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组，继承WritableComparator实现简单
 * Created by yushuanghe on 2017/02/14.
 */
public class SecondarySortGrouping extends WritableComparator {
    public SecondarySortGrouping() {
        super(SecondarySortPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SecondarySortPair key1 = (SecondarySortPair) a;
        SecondarySortPair key2 = (SecondarySortPair) b;
        int result = key1.getFirst().compareTo(key2.getFirst());
        if (result == 0) {
            return ((SecondarySortPair) a).compareTo((SecondarySortPair) b);
        }
        return result;
    }
}
