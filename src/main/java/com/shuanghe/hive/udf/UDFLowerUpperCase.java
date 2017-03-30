package com.shuanghe.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 自定义UDF，必须集成UDF，并重载实现evaluate方法
 * Created by yushuanghe on 2017/02/20.
 */
public class UDFLowerUpperCase extends UDF {

    /**
     * 对参数in进行大小写转换，
     * lowerOrUpper为lower，进行小写转换
     * lowerOrUpper为upper，进行大写转换
     *
     * @param in
     * @return
     */
    public Text evaluate(Text in, Text lowerOrUpper) {
        Text result = null;
        if ("lower".equals(lowerOrUpper.toString())) {
            result = new Text(in.toString().toLowerCase());
        } else if ("upper".equals(lowerOrUpper.toString())) {
            result = new Text(in.toString().toUpperCase());
        } else {
            result = in;
        }
        return result;
    }

    public Text evaluate(Text in) {
        return evaluate(in, new Text("lower"));
    }
}
