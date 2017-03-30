package com.shuanghe.hadoop.shuffle;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义map输出key
 * Created by yushuanghe on 2017/02/14.
 */
public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {
        super();
    }

    public IntPair(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }

    /**
     * 默认sort，group调用此方法
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(IntPair o) {
        if (o == this) {
            return 0;
        }
        int tmp = Integer.compare(this.first, o.first);
        if (tmp != 0)
            return tmp;
        tmp = Integer.compare(this.second, o.second);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
