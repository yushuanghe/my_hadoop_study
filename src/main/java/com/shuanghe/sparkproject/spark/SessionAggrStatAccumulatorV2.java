package com.shuanghe.sparkproject.spark;

import com.shuanghe.sparkproject.constant.Constants;
import org.apache.spark.util.AccumulatorV2;

/**
 * Created with IntelliJ IDEA.
 *
 * @author yushuanghe
 * Date: 2018/05/28
 * Time: 15:51
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class SessionAggrStatAccumulatorV2 extends AccumulatorV2<String, String> {

    /**
     * 初始值
     */
    private String value = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    /**
     * 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序
     *
     * @return
     */
    @Override
    public boolean isZero() {
        return false;
    }

    /**
     * 拷贝一个新的AccumulatorV2
     *
     * @return
     */
    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulatorV2 accumulator = new SessionAggrStatAccumulatorV2();
        accumulator.value = this.value;
        return accumulator;
    }

    /**
     * 重置AccumulatorV2中的数据
     */
    @Override
    public void reset() {
        value = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 操作数据累加方法实现
     *
     * @param v
     */
    @Override
    public void add(String v) {
        String v1 = value;
        String v2 = v;
        SessionAggrStatAccumulator.add(v1, v2);
    }

    /**
     * 合并数据
     *
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        value = other.value();
    }

    /**
     * AccumulatorV2对外访问的数据结果
     *
     * @return
     */
    @Override
    public String value() {
        return value;
    }
}