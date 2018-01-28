package homework;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WeiboUtil implements WritableComparable<WeiboUtil>{
    private int id;
    private int postId;
    private int score;
    private String text="";
    private String time="";
    private String userId="";

    public void setTime(String time) {
        this.time = time;
    }

    public String getTime() {

        return time;
    }

    public WeiboUtil() {
    }

    @Override

    public String toString() {
        return "WeiboUtil{" +
                "id=" + id +
                ", PostId=" + postId +
                ", score=" + score +
                ", text='" + text + '\'' +
                ", time=" + time +
                ", userId='" + userId + '\'' +
                '}';
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserId() {

        return userId;
    }

    public WeiboUtil(String sm) {
        String []strings=sm.trim().substring(5,sm.trim().length()-3).split("\"");
        System.out.println(strings);
        for(int i=0;i<strings.length-1;i++){
            switch (strings[i].trim()){
                case "Id=": this.id=Integer.parseInt(strings[i+1]);break;
                case  "PostId=":this.postId=Integer.parseInt(strings[i+1]);break;
                case "Score=": this.score=Integer.parseInt(strings[i+1]);break;
                case  "Text=":this.text=strings[i+1];break;
                case "CreationDate=": this.time=strings[i+1];break;
                case "UserId=":this.userId=strings[i+1];break;

            }
        }
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setPostId(int postId) {
        postId = postId;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getId() {

        return id;
    }

    public int getPostId() {
        return postId;
    }

    public int getScore() {
        return score;
    }

    public String getText() {
        return text;
    }

    @Override
    public int compareTo(WeiboUtil o) {
        System.out.println(this.time+"=================");
        System.out.println(this.time.length());
        String s1=this.time.substring(11,13);
        System.out.println(s1);
        String s2=o.time.substring(11,13);
        System.out.println(s2);
        if(o.time.substring(8,10).equals(this.time.substring(8, 10))){
            return s2.compareTo(s1);
        }
        return this.time.substring(8,10).compareTo(o.time.substring(8,10));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(postId);
        out.writeInt(score);
        out.writeUTF(text);
        out.writeUTF(time);
        out.writeUTF(userId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id=in.readInt();
        this.postId=in.readInt();
        this.score=in.readInt();
        this.text=in.readUTF();
        this.time=in.readUTF();
        this.userId=in.readUTF();
    }
}
