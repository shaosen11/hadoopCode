package cn.edu.lingnan.wordcound.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 15:27 2020/4/26
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosatguigu;
    FSDataOutputStream fosother;
    public FRecordWriter(TaskAttemptContext job){
        try {
            //1.获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //2.创建输出到atguigu.log的输出流
            fosatguigu = fs.create(new Path("D:/hadoop/output/outputformat/atguigu.log"));
            //3.创建输出到other.log的输出流
            fosother = fs.create(new Path("D:/hadoop/output/outputformat/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断key当中是否有奥拓福福，如果有写出到atguigu.log,如果没有写出到other.log
        if (text.toString().contains("atguigu")){
            fosatguigu.write(text.toString().getBytes());
        }else {
            fosother.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fosatguigu);
        IOUtils.closeStream(fosother);
    }
}
