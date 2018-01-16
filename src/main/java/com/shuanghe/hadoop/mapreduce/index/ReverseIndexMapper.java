package com.shuanghe.hadoop.mapreduce.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {
    private Logger logger=Logger.getLogger(ReverseIndexMapper.class);

    private Text word;
    private Text outputValue;
    private String filePath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word = new Text();
        outputValue = new Text();

        //获取分片信息
        FileSplit split = (FileSplit) context.getInputSplit();
        //从分片信息得到路径
        Path path = split.getPath();
        filePath = path.toString();
        //filePath=filePath.substring(filePath.length()-2,filePath.length()-1);
        logger.info(filePath);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            outputValue.set(filePath + ":1");
            context.write(word, outputValue);
        }
    }
}
