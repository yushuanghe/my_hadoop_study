package com.shuanghe.hadoop.mapreduce.secondarySort;

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
    private String second;

    public IntPair() {
        super();
    }

    public IntPair(int first, String second) {
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
        if (tmp == 0) {
            tmp = this.second.compareTo(o.second);
        }
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.first);
        out.writeUTF(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readUTF();
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }
}
