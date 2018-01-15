package com.shuanghe.hadoop.mapreduce.secondarySort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-1-15
 * Time: 下午4:11
 * To change this template use File | Settings | File Templates.
 * Description:自定义map输出key
 */
public class SecondarySortPair implements WritableComparable<SecondarySortPair> {
    private String first;
    private int second;

    public SecondarySortPair(){
    }

    public SecondarySortPair(String first, int second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 默认sort，group调用此方法
     * @param o
     * @return
     */
    @Override
    public int compareTo(SecondarySortPair o) {
        if (o == this) {
            return 0;
        }
        int result=this.first.compareTo(o.first);
        if(result==0){
            result=Integer.compare(this.second,o.second);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first=in.readUTF();
        this.second=in.readInt();
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SecondarySortPair)) return false;

        SecondarySortPair that = (SecondarySortPair) o;

        if (getSecond() != that.getSecond()) return false;
        return getFirst() != null ? getFirst().equals(that.getFirst()) : that.getFirst() == null;
    }

    @Override
    public int hashCode() {
        int result = getFirst() != null ? getFirst().hashCode() : 0;
        result = 31 * result + getSecond();
        return result;
    }

    @Override
    public String toString() {
        return "SecondarySortPair{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }
}