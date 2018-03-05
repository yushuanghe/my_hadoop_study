package com.shuanghe.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 日期格式化
 * Created by yushuanghe on 2018/02/07.
 */
public class FormatDateUDF extends UDF {

    private final SimpleDateFormat inFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Text evaluate(Text text) {
        Text result = null;
        if (text != null && StringUtils.isNotBlank(text.toString().trim())) {
            String str = text.toString().trim();
            try {
                Date date = inFormat.parse(str);
                result = new Text(outFormat.format(date));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    //public static void main(String[] args) {
    //    Text text=new Text("31/Aug/2015:00:04:37 +0800");
    //    System.out.println(new FormatDateUDF().evaluate(text));
    //}
}
