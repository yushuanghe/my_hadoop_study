package com.shuanghe.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * 自定义UDAF函数实现
 * <p/>
 * AbstractGenericUDAFResolver 类主要作用就是根据hql调用时候的函数参数来获取具体的 GenericUDAFEvaluator 实例对象
 * Created by yushuanghe on 2017/02/23.
 */
public class UDAFSumCase2 extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns()) {
            //函数允许使用'*'查询的时候，会返回true
            throw new SemanticException("不支持使用*查询");
        }

        //获取参数列表
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        if (inspectors.length != 1) {
            throw new UDFArgumentException("只支持一个参数进行查询");
        }

        //获取参数，只有一个参数
        AbstractPrimitiveWritableObjectInspector apwoi =
                (AbstractPrimitiveWritableObjectInspector) inspectors[0];
        switch (apwoi.getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                //进行整型的sum操作
                return new SumLongEvaluator();

            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case VARCHAR:
            case CHAR:
                //进行浮点型sum操作
                return new SumDoubleEvaluator();

            default:
                throw new UDFArgumentException("参数类型异常");

        }
    }

//    @Override
//    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
//        if (info.length != 1) {
//            throw new UDFArgumentException("只支持一个参数查询");
//        }
//
//        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
//            case BYTE:
//            case SHORT:
//            case INT:
//            case LONG:
//                return new SumLongEvaluator();
//            case TIMESTAMP:
//            case FLOAT:
//            case DOUBLE:
//            case STRING:
//            case VARCHAR:
//            case CHAR:
//                return new SumDoubleEvaluator();
//            default:
//                throw new UDFArgumentException("参数类型异常");
//        }
//
//    }

    /**
     * 进行整型sum操作
     * <p/>
     * GenericUDAFEvaluator 类主要作用就是根据job的不同阶段执行不同的方法
     */
    static class SumLongEvaluator extends GenericUDAFEvaluator {

        //参数类型
        private PrimitiveObjectInspector inputOI;

        /**
         * 存储sum的值的类
         * 自定义AggregationBuffer
         */
        static class SumLongAgg implements AggregationBuffer {
            long sum;
            boolean empty;
        }

        /**
         * 这个方法返回了UDAF的返回类型，这里确定了sum自定义函数的返回类型是Long类型
         * 根据 GenericUDAFEvaluator.Model（内部类） 来确定job的执行阶段
         *
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (parameters.length != 1) {
                throw new UDFArgumentException("参数数量异常");
            }
            inputOI = (PrimitiveObjectInspector) parameters[0];
            //PrimitiveObjectInspector用来完成对基本数据类型的解析
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        /**
         * 创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
         *
         * @return
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumLongAgg sla = new SumLongAgg();
            //一般进行一下reset操作
            this.reset(sla);
            return sla;
        }

        /**
         * mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
         *
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumLongAgg sla = (SumLongAgg) agg;
            sla.sum = 0L;
            sla.empty = true;
        }

        /**
         * map阶段调用，只要把保存当前和的对象agg，再加上输入的参数，就可以了。
         * 循环处理调用的方法
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数数量异常");
            }
            this.merge(agg, parameters[0]);
        }

        /**
         * mapper结束要返回的结果，还有combiner结束返回的结果
         * 部分聚合后的数据输出
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        /**
         * combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
         * 合并操作
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumLongAgg sla = (SumLongAgg) agg;
                sla.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
                sla.empty = false;
            }
        }

        /**
         * reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumLongAgg sla = (SumLongAgg) agg;
            if (sla.empty) {
                return null;
            }
            return new LongWritable(sla.sum);
        }
    }

    /**
     * 进行浮点型sum操作
     * <p/>
     * GenericUDAFEvaluator 类主要作用就是根据job的不同阶段执行不同的方法
     */
    static class SumDoubleEvaluator extends GenericUDAFEvaluator {

        //参数类型
        private PrimitiveObjectInspector inputOI;

        /**
         * 存储sum的值的类
         * 自定义AggregationBuffer
         */
        static class SumDoubleAgg implements AggregationBuffer {
            double sum;
            boolean empty;
        }

        /**
         * 这个方法返回了UDAF的返回类型，这里确定了sum自定义函数的返回类型是Double类型
         * 根据 GenericUDAFEvaluator.Model（内部类） 来确定job的执行阶段
         *
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (parameters.length != 1) {
                throw new UDFArgumentException("参数个数不符");
            }

            inputOI = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }

        /**
         * 创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
         *
         * @return
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumDoubleAgg sda = new SumDoubleAgg();
            //一般进行一下reset操作
            this.reset(sda);
            return sda;
        }

        /**
         * mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
         *
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg sda = (SumDoubleAgg) agg;
            sda.sum = 0;
            sda.empty = true;
        }

        /**
         * map阶段调用，只要把保存当前和的对象agg，再加上输入的参数，就可以了。
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数数量异常");
            }
            this.merge(agg, parameters[0]);
        }

        /**
         * mapper结束要返回的结果，还有combiner结束返回的结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        /**
         * combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumDoubleAgg sda = (SumDoubleAgg) agg;
                sda.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
                sda.empty = false;
            }
        }

        /**
         * reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg sda = (SumDoubleAgg) agg;
            if (sda.empty) {
                return null;
            }
            return new DoubleWritable(sda.sum);
        }
    }
}
