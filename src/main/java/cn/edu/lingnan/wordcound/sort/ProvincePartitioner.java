package cn.edu.lingnan.wordcound.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 20:29 2020/4/25
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean bean, Text text, int i) {
        //按照手机号的前三位
        String prePhoneNum = text.toString().substring(0, 3);

        int partition = 4;

        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }
        return partition;
    }
}
