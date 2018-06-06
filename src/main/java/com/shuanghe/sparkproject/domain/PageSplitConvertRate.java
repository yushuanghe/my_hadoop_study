package com.shuanghe.sparkproject.domain;

/**
 * Description:页面切片转化率
 * <p>
 * Date: 2018/06/06
 * Time: 18:22
 *
 * @author yushuanghe
 */
public class PageSplitConvertRate {
    private long taskid;
    private String convertRate;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}