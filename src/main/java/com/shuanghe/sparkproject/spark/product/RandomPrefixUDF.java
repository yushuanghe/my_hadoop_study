package com.shuanghe.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Description:random_prefix()
 * <p>
 * Date: 2018/06/07
 * Time: 18:04
 *
 * @author yushuanghe
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    private static final long serialVersionUID = -9200710869363738080L;

    @Override
    public String call(String val, Integer integer) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(integer);
        return randNum + "_" + val;
    }
}