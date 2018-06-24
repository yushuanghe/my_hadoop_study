package com.shuanghe.hive.udtf;

import com.shuanghe.mapreduceAndHbase.HBaseTableOnlyMapperDemo;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

/**
 * UDTF(User-Defined Table-Generating Function)支持一个输入多个输出。
 * 一般用于解析工作，比如说解析url，然后获取url中的信息。
 * 要求继承类org.apache.hadoop.hive.ql.udf.generic.GenericUDTF，
 * 实现方法：initialize(返回返回值的参数类型)、
 * process具体的处理方法，一般在这个方法中会调用父类的forward方法进行数据的写出、
 * close关闭资源方法，最终会调用close方法，同MR程序中的cleanUp方法。
 * <p>
 * Created by yushuanghe on 2017/02/25.
 */
public class UDTFCase extends GenericUDTF {

    /**
     * 返回返回值的类型
     * 该方法指定输入输出参数：输入的Object Inspectors和输出的Struct。
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
     * 该方法处理输入记录，然后通过forward()方法返回输出结果。
     *
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
        Map<String, String> map = HBaseTableOnlyMapperDemo.transfoerContent2Map(line);

        String[] strs = new String[]{map.get("p_id"), map.get("p_name"), map.get("price")};

        //List<String> result = new ArrayList<>();
        //result.add(map.get("p_id"));
        //result.add(map.get("p_name"));
        //result.add(map.get("price"));

        //调用父类forward方法写出
        //super.forward(result.toArray(new String[0]));
        super.forward(strs);
    }

    /**
     * forward可以在process和close方法中调用，可以调用多次
     * 该方法用于通知UDTF没有行可以处理了。可以在该方法中清理代码或者附加其他处理输出。
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {
        //nothing
        super.forward(new String[]{"123456789", "close", "123"});
    }
}
