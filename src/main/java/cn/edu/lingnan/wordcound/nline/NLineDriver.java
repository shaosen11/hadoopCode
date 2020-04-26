package cn.edu.lingnan.wordcound.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 14:13 2020/4/25
 */
public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/hadoop/input/nlineinput", "D:/hadoop/output/nlineinput"};

        //1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //7.设置每个切片inputSplit中划分的三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        //8.使用NlineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        //2.设置jar包位置
        job.setJarByClass(NLineDriver.class);

        //3.关联mapper和reducer类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        //4.设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.输入输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.作业提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 1 : 0);
    }
}
