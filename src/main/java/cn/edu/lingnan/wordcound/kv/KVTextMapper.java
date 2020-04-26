package cn.edu.lingnan.wordcound.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:13 2020/4/24
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //1.封装对象

        //2.写出
        context.write(key, intWritable);
    }
}
