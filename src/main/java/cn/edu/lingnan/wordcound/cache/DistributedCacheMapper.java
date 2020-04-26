package cn.edu.lingnan.wordcound.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 18:03 2020/4/26
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //1切割
            String[] fileds = line.split("\t");
            pdMap.put(fileds[0], fileds[1]);
        }
        //2.关闭资源
        IOUtils.closeStream(reader);
    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        String[] fields = line.split("\t");

        //.获取pid
        String pid = fields[1];

        //3.取出pname
        String pname = pdMap.get(pid);

        //4.拼接
        line = line + '\t' + pname;

        k.set(line);

        //5.写出
        context.write(k, NullWritable.get());
    }
}
