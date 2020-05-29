package cn.edu.lingnan.wordcound.maxmin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 22:45 2020/4/28
 */
public class MaxMinPartition extends Partitioner<MaxMinBean, NullWritable> {

    @Override
    public int getPartition(MaxMinBean maxMinBean, NullWritable nullWritable, int i) {
        String s = maxMinBean.getMouth().substring(0, 4);
        int result = 0;
        if ("2017".equals(s)) {
            result = 0;
        } else if ("2018".equals(s)) {
            result = 1;
        }
        return result;
    }
}
