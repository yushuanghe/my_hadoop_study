package com.shuanghe.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 去除字符串双引号
 * Created by yushuanghe on 2018/02/07.
 */
public class RemoveQuotesUDF extends UDF {

    public Text evaluate(Text text) {
        Text result=null;
        if(text!=null&& StringUtils.isNotBlank(text.toString())){
            result=new Text(text.toString().replaceAll("\"",""));
        }
        return result;
    }

    //public static void main(String[] args) {
    //    Text text=new Text("\"asdasdsa\"");
    //    System.out.println(new RemoveQuotesUDF().evaluate(text));
    //}
}
