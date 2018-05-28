/**
 * spark项目
 * <p>
 * 按时间抽取hive表中数据,与user表join
 * 做session粒度聚合
 * 根据条件过滤数据
 * 使用自定义 Accumulator 计算session的时长,步长
 * 按session数量比例随机抽取部分session
 *
 * @author yushuanghe
 */
package com.shuanghe.sparkproject;