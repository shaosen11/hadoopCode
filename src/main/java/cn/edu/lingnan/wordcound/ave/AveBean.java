package cn.edu.lingnan.wordcound.ave;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 17:45 2020/4/28
 */
public class AveBean implements Writable {
    private Integer sum;
    private Integer count;

    public AveBean() {
    }


    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum = dataInput.readInt();
        count = dataInput.readInt();
    }
}
