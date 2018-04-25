package com.shuanghe.spark.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

class StringCountUDAF extends UserDefinedAggregateFunction {
    //输入数据的类型
    override def inputSchema: StructType = StructType(Array(StructField("str", DataTypes.StringType, true)))

    //中间进行聚合时，所处理的数据的类型
    override def bufferSchema: StructType = StructType(Array(StructField("count", DataTypes.IntegerType, true)))

    //函数返回之的类型
    override def dataType: DataType = DataTypes.IntegerType

    //
    override def deterministic: Boolean = {
        true
    }

    //为每个分组的数据执行初始化操作
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0
    }

    //每个分组有新的值进来的时候，如何进行分组对应的值的计算
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Int](0) + 1
    }

    //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，也就是update
    //所以，最后一个分组，在各个节点上的聚合值，要进行merge
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    }

    //一个分组的聚合值，如果通过中间的缓存聚合值，最后返回一个最终的聚合值
    override def evaluate(buffer: Row): Any = {
        buffer.getAs[Int](0)
    }
}
