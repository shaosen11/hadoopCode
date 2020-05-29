package cn.edu.lingnan.wordcound.maxmin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 22:33 2020/4/28
 */
public class MaxMinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MaxMinDriver.class);
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);

        job.setMapOutputKeyClass(MaxMinBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(MaxMinBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);
        job.setPartitionerClass(MaxMinPartition.class);

        job.setGroupingComparatorClass(MaxMinComparator.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop112:9000/maxmin"));
        FileOutputFormat.setOutputPath(job, new Path("D:/hadoop/output/maxmin5"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 1 : 0);
    }
}
