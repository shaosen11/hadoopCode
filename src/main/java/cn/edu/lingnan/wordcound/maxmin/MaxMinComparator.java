package cn.edu.lingnan.wordcound.maxmin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 0:58 2020/4/29
 */
public class MaxMinComparator extends WritableComparator {
    public MaxMinComparator() {
        super(MaxMinBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        MaxMinBean aBean = (MaxMinBean) a;
        MaxMinBean bBean = (MaxMinBean) b;

        return aBean.getMouth().compareTo(bBean.getMouth());
    }
}
