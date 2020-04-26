package cn.edu.lingnan.wordcound.nline;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 14:03 2020/4/25
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.切割
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
