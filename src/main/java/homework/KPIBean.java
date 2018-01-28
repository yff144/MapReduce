package homework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KPIBean extends WritableComparator  implements WritableComparable<KPIBean>{
    private String browser="";
    private String ips="";
    private String time="";

    public KPIBean() {
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String sa=a.toString().substring(19,21);
        String sb=b.toString().substring(19,21);
        return sa.compareTo(sb);
    }

    public String getBrowser() {
        return browser;
    }

    public String getIps() {
        return ips;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public void setIps(String ips) {
        this.ips = ips;
    }


    @Override
    public String toString() {
        return "KPIBean{" +
                "browser='" + browser + '\'' +
                ", ips='" + ips + '\'' +
                '}';
    }


    @Override
    public int compareTo(KPIBean o) {
        if(this.browser.equals(o.browser)){
            return this.ips.compareTo(o.ips);
        }
        return this.browser.compareTo(o.browser);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(browser);
        out.writeUTF(ips);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.browser=in.readUTF();
        this.ips=in.readUTF();
    }
}
