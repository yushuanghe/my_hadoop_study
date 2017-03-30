package com.shuanghe.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yushuanghe on 2017/02/13.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Text word;
    private LongWritable one;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("WordCountMapper调用map方法");

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word = new Text();
        one = new LongWritable(1);
        System.out.println("WordCountMapper调用setup方法");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountMapper调用cleanup方法");
    }
}
