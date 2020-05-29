package cn.edu.lingnan.wordcound.ave;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 17:21 2020/4/28
 */
public class AveCombiner extends Reducer<Text, AveBean, Text, AveBean> {
    int sum = 0;
    int count = 0;
    Text k = new Text();
    AveBean aveBean = new AveBean();

    @Override
    protected void reduce(Text key, Iterable<AveBean> values, Context context) throws IOException, InterruptedException {
        //统计每个班级的总信息
        for (AveBean value : values) {
            sum += value.getSum();
            count += value.getCount();
        }
        System.out.println(sum);
        System.out.println(count);
        aveBean.setSum(sum);
        aveBean.setCount(count);
        //把key设置为2017级
        k.set(key.toString().substring(0,4));
        context.write(k, aveBean);
    }
}
