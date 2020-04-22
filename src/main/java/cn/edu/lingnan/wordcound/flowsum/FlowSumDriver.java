package cn.edu.lingnan.wordcound.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 23:33 2020/4/22
 */
public class FlowSumDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        args = new String[]{"D:/hadoop/input/phone_data.txt", "D:/hadoop/output/flowsum"};
        Configuration conf = new Configuration();
        // 1 获取job对象
        Job job = Job.getInstance(conf);
        // 2 设置jar的路径
        job.setJarByClass(FlowSumDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowConutReducer.class);

        // 4 设置mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
