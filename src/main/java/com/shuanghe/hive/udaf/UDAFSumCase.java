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
import org.apache.hadoop.io.LongWritable;

/**
 * 自定义UDAF函数实现
 * <p/>
 * AbstractGenericUDAFResolver 类主要作用就是根据hql调用时候的函数参数来获取具体的 GenericUDAFEvaluator 实例对象，也就是说实现方法 getEvaluator 即可
 * <p>
 * 测试sql
 * add jar /home/yushuanghe/test/jar/my_hadoop_study.jar;
 * create temporary function sum_case as 'com.shuanghe.hive.udaf.UDAFSumCase';
 * <p>
 * select sum_case(sal) as sal,sum_case(empno) as empno from emp group by deptno;
 * select sum_case(deptno) from emp;
 * <p>
 * drop temporary function sum_case;
 * delete jar /home/yushuanghe/test/jar/my_hadoop_study.jar;
 * <p>
 * Created by yushuanghe on 2017/02/23.
 */
public class UDAFSumCase extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns()) {
            //函数允许使用'*'查询的时候，会返回true
            throw new SemanticException("不支持使用*查询");
        }

        //获取函数参数列表
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        if (inspectors.length != 1) {
            throw new UDFArgumentException("只支持一个参数进行查询");
        }

        //获取参数，只有一个参数
        AbstractPrimitiveWritableObjectInspector apwoi = (AbstractPrimitiveWritableObjectInspector) inspectors[0];
        switch (apwoi.getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                //进行整型的sum操作
                return new SumLongEvaluator();

            case FLOAT:
            case DOUBLE:
                //进行浮点型sum操作
                return new SumDoubleEvaluator();

            default:
                throw new UDFArgumentException("参数类型异常");

        }
    }

    /**
     * 进行整型sum操作
     * <p/>
     * GenericUDAFEvaluator 类主要作用就是根据job的不同阶段执行不同的方法
     * hive通过GenericUDAFEvaluator.Model来确定job的执行阶段。
     * PARTIAL1：从原始数据到部分聚合，会调用方法iterate和terminatePartial方法；(map)
     * PARTIAL2：从部分数据聚合和部分数据聚合，会调用方法merge和terminatePartial；(combiner)
     * FINAL：从部分数据聚合到全部数据聚合，会调用方法merge和terminate；(reduce)
     * COMPLETE：从原始数据到全部数据聚合，会调用方法iterate和terminate。(只有map,无reudce)
     * <p>
     * 除了上面提到的iterate、merge、terminate和terminatePartial以外，还有init(初始化并返回返回值的类型)、getNewAggregationBuffer(获取新的buffer对象，也就是方法之间传递参数的对象)，reset(重置buffer对象)。
     */
    static class SumLongEvaluator extends GenericUDAFEvaluator {

        /**
         * 参数类型
         */
        private PrimitiveObjectInspector inputOI;

        /**
         * 自定义AggregationBuffer
         */
        static class SumLongAgg implements AggregationBuffer {
            long sum;
            boolean empty;
        }

        /**
         * 初始化并返回返回值的类型
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
            //PrimitiveObjectInspector 用来完成对基本数据类型的解析
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        /**
         * 获取新的buffer对象，也就是方法之间传递参数的对象
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
         * 重置buffer对象
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
         * 合并操作
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            //被合并的值
            if (partial != null) {
                SumLongAgg sla = (SumLongAgg) agg;
                sla.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
                sla.empty = false;
            }
        }

        /**
         * 全部聚合后的数据输出
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

        /**
         * 参数类型
         */
        private PrimitiveObjectInspector inputOI;

        /**
         * 自定义AggregationBuffer
         */
        static class SumDoubleAgg implements AggregationBuffer {
            double sum;
            boolean empty;
        }

        /**
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

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumDoubleAgg sda = new SumDoubleAgg();

            //一般进行一下reset操作
            this.reset(sda);
            return sda;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg sda = (SumDoubleAgg) agg;
            sda.sum = 0;
            sda.empty = true;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数数量异常");
            }

            this.merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumDoubleAgg sda = (SumDoubleAgg) agg;
                sda.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
                sda.empty = false;
            }
        }

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
