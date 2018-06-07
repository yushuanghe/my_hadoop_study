package com.shuanghe.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * Description:将两个字段拼接起来（使用指定的分隔符）
 * <p>
 * Date: 2018/06/07
 * Time: 17:12
 *
 * @author yushuanghe
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    private static final long serialVersionUID = -3342211742872981400L;

    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return String.valueOf(v1) + split + v2;
    }
}