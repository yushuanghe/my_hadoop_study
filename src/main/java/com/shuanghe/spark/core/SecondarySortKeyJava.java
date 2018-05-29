package com.shuanghe.spark.core;

import scala.Serializable;
import scala.math.Ordered;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-29
 * Time: 上午12:12
 * To change this template use File | Settings | File Templates.
 * Description:自定义二次排序key
 */
public class SecondarySortKeyJava implements Ordered<SecondarySortKeyJava>, Serializable {
    /**
     * 在自定义key里定义要进行排序的列，需要getter，setter，hashcode，equals方法
     */
    private int first;
    private int second;

    public SecondarySortKeyJava(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondarySortKeyJava that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortKeyJava that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKeyJava that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKeyJava that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKeyJava that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortKeyJava that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SecondarySortKeyJava)) {
            return false;
        }

        SecondarySortKeyJava that = (SecondarySortKeyJava) o;

        if (getFirst() != that.getFirst()) {
            return false;
        }
        return getSecond() == that.getSecond();
    }

    @Override
    public int hashCode() {
        int result = getFirst();
        result = 31 * result + getSecond();
        return result;
    }
}