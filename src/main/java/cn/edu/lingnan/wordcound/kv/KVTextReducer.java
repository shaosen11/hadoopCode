package cn.edu.lingnan.wordcound.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:16 2020/4/24
 */
public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //1.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        //2.写出
        context.write(key, v);
    }
}
