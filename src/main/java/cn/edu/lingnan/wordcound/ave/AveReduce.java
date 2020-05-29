package cn.edu.lingnan.wordcound.ave;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 17:18 2020/4/28
 */
public class AveReduce extends Reducer<Text, AveBean, DoubleWritable, NullWritable> {
    int sum = 0;
    int count = 0;
    double ave = 0;
    DoubleWritable k = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<AveBean> values, Context context) throws IOException, InterruptedException {
        for (AveBean value : values) {
            sum += value.getSum();
            count += value.getCount();
        }
        ave = (double)sum/(double)count;
        System.out.println("年级数学平均分" + ave);
        k.set(ave);
        context.write(k, NullWritable.get());
    }
}
