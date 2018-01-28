package homework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class MRDPUtils implements WritableComparable<MRDPUtils> {
    private String id="";
    private String posit="";
    private  String timeDuring="";
    private  Date time=new Date();

    public MRDPUtils(String id, String posit, Date time) {
        this.id = id;
        this.posit = posit;
        this.time = time;
        this.timeDuring=this.shiduan();
    }

    @Override
    public String toString() {
        return "用户"+this.id+"在"+this.timeDuring+"点这个时间段在基站"+this.posit+"停留了";
    }

    public MRDPUtils() {
    }

    public  String shiduan(){
        String str=null;
        String []s={"09","05","12"};
        Arrays.sort(s);
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(this.time);
        int hour=calendar.get(Calendar.HOUR_OF_DAY);
        System.out.println(hour);
        for(int i=0;i<s.length;i++){
            if(hour<=Integer.parseInt(s[i])){
                if(i==0){
                    str= "00-"+s[i];
                    break;
                }
                str=s[i-1]+"-"+s[i];
                break;
            }

        }
        return str;
    }

    @Override
    public int compareTo(MRDPUtils o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(posit);
        out.writeUTF(timeDuring);

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPosit() {
        return posit;
    }

    public void setPosit(String posit) {
        this.posit = posit;
    }

    public String getTimeDuring() {
        return timeDuring;
    }

    public void setTimeDuring(String timeDuring) {
        this.timeDuring = timeDuring;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id=in.readUTF();
        this.posit=in.readUTF();
        this.timeDuring=in.readUTF();

    }
}
