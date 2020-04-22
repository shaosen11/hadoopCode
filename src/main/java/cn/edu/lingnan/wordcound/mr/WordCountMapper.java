package cn.edu.lingnan.wordcound.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:03 2020/4/22
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1获取一行
        String line = value.toString();

        //2切割单词
        String[] words = line.split(" ");

        //3循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }

    }
}
