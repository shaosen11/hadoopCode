package cn.edu.lingnan.wordcound.ave;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 18:03 2020/4/28
 */
public class AveDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(AveDriver.class);
        job.setMapperClass(AveMapper.class);
        job.setCombinerClass(AveCombiner.class);
        job.setReducerClass(AveReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AveBean.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop112:9000/aveage"));
        FileOutputFormat.setOutputPath(job, new Path("D:/hadoop/output/ave"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 1 : 0);
    }
}
