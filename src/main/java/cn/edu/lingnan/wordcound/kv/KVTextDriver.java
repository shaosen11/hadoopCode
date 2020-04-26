package cn.edu.lingnan.wordcound.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:20 2020/4/24
 */
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/hadoop/input/shaosen", "D:/hadoop/output/kvtextinputformat"};
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar存储路径
        job.setJarByClass(KVTextDriver.class);

        //3.关联mapper和reducer类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        //4.设置mapper输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
