package cn.edu.lingnan.wordcound.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author shaosen
 * @Description //TODO
 * @Date 21:05 2020/4/25
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id;
    private double price;

    public OrderBean() {
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {
            if (price > o.getPrice()) {
                result = -1;
            } else if (price < o.getPrice()) {
                result = 1;
            } else {
                result = 0;
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}
