package com.shuanghe.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

/**
 * Created by yushuanghe on 2017/02/25.
 */
public class UDTFCase extends GenericUDTF {

    /**
     * 返回返回值的类型
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("参数数量异常");
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("id");
        fieldNames.add("name");
        fieldNames.add("price");

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args.length != 1) {
            return;
        }
        //只有一个参数
        String line = args[0].toString();
        Map<String, String> map = transfoerContent2Map(line);

        List<String> result = new ArrayList<>();
        result.add(map.get("p_id"));
        result.add(map.get("p_name"));
        result.add(map.get("price"));

        //调用父类forward方法写出
        super.forward(result.toArray(new String[0]));
    }

    /**
     * forward可以在process和close方法中调用，可以调用多次
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {
        //nothing
        super.forward(new String[]{"123456789", "close", "123"});
    }

    /**
     * 转换content为map对象
     *
     * @param content
     * @return
     */
    static Map<String, String> transfoerContent2Map(String content) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (tokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                //当前值为value
                map.put(key, tokenizer.nextToken());
            } else {
                //当前值为key
                key = tokenizer.nextToken();
            }
        }
        return map;
    }
}
