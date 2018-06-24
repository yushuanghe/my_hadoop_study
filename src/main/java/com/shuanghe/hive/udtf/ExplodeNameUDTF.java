package com.shuanghe.hive.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * Description: 自定义UDTF
 * <p>
 * 测试sql
 * add jar /home/yushuanghe/test/jar/my_hadoop_study.jar;
 * create temporary function explode_name as 'com.shuanghe.hive.udtf.ExplodeNameUDTF';
 * desc function explode_name;
 * <p>
 * select explode_name(ename) from emp;
 * select explode_name(ename) as (a,b) from emp;
 * <p>
 * drop temporary function explode_name;
 * delete jar /home/yushuanghe/test/jar/my_hadoop_study.jar;
 * <p>
 * Date: 2018/06/24
 * Time: 14:14
 *
 * @author yushuanghe
 */
@Description(
        name = "explode_name",
        value = "_FUNC_(col) - The parameter is a column name."
                + " The return value is two strings.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) FROM src;"
                + " > SELECT _FUNC_(col) AS (name, surname) FROM src;"
                + " > SELECT adTable.name,adTable.surname"
                + " > FROM src LATERAL VIEW _FUNC_(col) adTable AS name, surname;"
)
public class ExplodeNameUDTF extends GenericUDTF {
    /**
     * 该方法指定输入输出参数：输入的Object Inspectors和输出的Struct。
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        if (argOIs.length != 1) {
            throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
        }

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("surname");
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
        String input = args[0].toString();
        String[] name = input.split(" ");
        super.forward(name);
    }

    /**
     * 该方法用于通知UDTF没有行可以处理了。可以在该方法中清理代码或者附加其他处理输出。
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {
        //nothing
    }
}