package com.shuanghe.sparkproject.model;

/**
 * Description:用户广告点击量查询结果
 * <p>
 * Date: 2018/06/08
 * Time: 16:26
 *
 * @author yushuanghe
 */
public class AdUserClickCountQueryResult {
    private int count;
    private int clickCount;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getClickCount() {
        return clickCount;
    }

    public void setClickCount(int clickCount) {
        this.clickCount = clickCount;
    }
}