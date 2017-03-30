package com.shuanghe.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yushuanghe on 2017/02/13.
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    //  private Text outputKey;
    private LongWritable outputValue;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("WordCountReducer调用reduce方法");

        long sum = 0;
        for (LongWritable value : values) {
            sum += 1;
        }
//    outputKey.set(key);
        outputValue.set(sum);
        context.write(key, outputValue);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//    outputKey = new Text();
        outputValue = new LongWritable();
        System.out.println("WordCountReducer调用setup方法");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountReducer调用cleanup方法");
    }
}
