package cn.edu.lingnan.wordcound.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @Author shaosen
 * @Description //TODO
 * @Date 16:21 2020/4/25
 */
public class ProvincePartition extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //获取手机号前三位
        String prePhomeNum = text.toString().substring(0, 3);

        int partition =4;
        if("136".equals(prePhomeNum)){
            partition = 0;
        }else if("137".equals(prePhomeNum)) {
            partition = 1;
        }else if("138".equals(prePhomeNum)) {
            partition = 2;
        }else if("139".equals(prePhomeNum)) {
            partition = 3;
        }
        return partition;
    }
}
