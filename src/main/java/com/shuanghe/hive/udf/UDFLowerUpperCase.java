package com.shuanghe.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 自定义UDF，必须继承UDF，并重载实现 evaluate 方法
 * Created by yushuanghe on 2017/02/20.
 */
public class UDFLowerUpperCase extends UDF {

    /**
     * 对参数in进行大小写转换，
     * lowerOrUpper为lower，进行小写转换
     * lowerOrUpper为upper，进行大写转换
     *
     * @param text
     * @return
     */
    public Text evaluate(Text text, Text lowerOrUpper) {
        Text result = null;
        if (text == null || StringUtils.isBlank(text.toString())) {
            return result;
        }

        if ("lower".equals(lowerOrUpper.toString())) {
            result = new Text(text.toString().toLowerCase());
        } else if ("upper".equals(lowerOrUpper.toString())) {
            result = new Text(text.toString().toUpperCase());
        } else {
            result = text;
        }
        return result;
    }

    public Text evaluate(Text text) {
        return evaluate(text, new Text("lower"));
    }
}
