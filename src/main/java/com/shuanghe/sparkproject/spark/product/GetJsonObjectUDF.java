package com.shuanghe.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * Description: get_json_object()
 * 技术点：自定义UDF函数
 * <p>
 * Date: 2018/06/07
 * Time: 17:33
 *
 * @author yushuanghe
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    private static final long serialVersionUID = -644935413478804197L;

    @Override
    public String call(String json, String field) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            return jsonObject.getString(field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}