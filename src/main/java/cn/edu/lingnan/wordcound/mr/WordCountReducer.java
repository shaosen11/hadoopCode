package cn.edu.lingnan.wordcound.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:32 2020/4/22
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //1累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        IntWritable v = new IntWritable();
        v.set(sum);
        //2写出
        context.write(key, v);
    }
}
