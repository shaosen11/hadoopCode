package cn.edu.lingnan.wordcound.maxmin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 22:29 2020/4/28
 */
public class MaxMinReducer extends Reducer<MaxMinBean, NullWritable, MaxMinBean, NullWritable> {
    MaxMinBean v = new MaxMinBean();

    @Override
    protected void reduce(MaxMinBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Integer max = null;
        Integer min = null;
        for (NullWritable value : values) {
            if (max == null) {
                max = key.getMax();
            }
            if (min == null) {
                min = key.getMin();
            }
            if (key.getMax() > max) {
                max = key.getMax();
            } else if (key.getMin() < min) {
                min = key.getMin();
            }
        }
        v.setMouth(key.getMouth());
        v.setMax(max);
        v.setMin(min);
        context.write(key, NullWritable.get());
    }
}
