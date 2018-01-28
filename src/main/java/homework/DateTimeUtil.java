package homework;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class DateTimeUtil implements Writable{
    private Date max=new Date(0);
    private Date min=new Date(0);
    private int count;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(max.getTime());
        out.writeLong(min.getTime());
        out.writeInt(count);
    }

    @Override
    public String toString() {
        return "DateTimeUtil{" +
                "max=" + max +
                ", min=" + min +
                ", count=" + count +
                '}';
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Date getMax() {

        return max;
    }

    public Date getMin() {
        return min;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.max=new Date(in.readLong());
        this.min=new Date(in.readLong());
        this.count=in.readInt();

    }
}
