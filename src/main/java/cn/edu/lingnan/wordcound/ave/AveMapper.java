package cn.edu.lingnan.wordcound.ave;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 17:13 2020/4/28
 */
public class AveMapper extends Mapper<LongWritable, Text, Text, AveBean> {
    Text k = new Text();
    AveBean v = new AveBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        for (String field : fields) {
            System.out.println(field);
        }
        //把班级作为主键
        String classNum = fields[0].substring(0, 6);
        k.set(classNum);
        v.setSum(Integer.parseInt(fields[1]));
        v.setCount(1);
        context.write(k, v);
    }
}
