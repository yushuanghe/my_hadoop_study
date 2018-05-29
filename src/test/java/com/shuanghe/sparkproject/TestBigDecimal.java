package com.shuanghe.sparkproject;

import com.shuanghe.util.NumberUtils;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 *
 * @author yushuanghe
 * Date: 2018/05/29
 * Time: 13:32
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class TestBigDecimal {
    @Test
    public void test1() {
        int num = 62;
        int sum = 985;

        double ratio = NumberUtils.formatDouble(
                (double) num / sum, 2);

        System.out.println(ratio);
    }
}