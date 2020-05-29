package cn.edu.lingnan.wordcound.maxmin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 22:22 2020/4/28
 */
public class MaxMinBean implements WritableComparable<MaxMinBean> {
    private String mouth;
    private Integer max;
    private Integer min;

    public MaxMinBean() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mouth);
        dataOutput.writeInt(max);
        dataOutput.writeInt(min);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mouth = dataInput.readUTF();
        max = dataInput.readInt();
        min = dataInput.readInt();
    }

    @Override
    public int compareTo(MaxMinBean o) {
        return o.mouth.compareTo(mouth);
    }

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public String getMouth() {
        return mouth;
    }

    public void setMouth(String mouth) {
        this.mouth = mouth;
    }

    @Override
    public String toString() {
        return mouth + "\t" + max + "\t" + min;
    }
}
