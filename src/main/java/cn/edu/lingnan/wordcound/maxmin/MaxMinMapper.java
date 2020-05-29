package cn.edu.lingnan.wordcound.maxmin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 22:24 2020/4/28
 */
public class MaxMinMapper extends Mapper<LongWritable, Text, MaxMinBean, NullWritable> {
    MaxMinBean k = new MaxMinBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        k.setMouth(fields[0]);
        k.setMax(Integer.parseInt(fields[1]));
        k.setMin(Integer.parseInt(fields[2]));
        context.write(k, NullWritable.get());
    }
}
