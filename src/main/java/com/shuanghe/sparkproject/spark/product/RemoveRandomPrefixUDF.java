package com.shuanghe.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Description:去除随机前缀
 * <p>
 * Date: 2018/06/07
 * Time: 18:05
 *
 * @author yushuanghe
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
    private static final long serialVersionUID = 8373088532608844715L;

    @Override
    public String call(String val) throws Exception {
        String[] valSplited = val.split("_");
        return valSplited[1];
    }
}