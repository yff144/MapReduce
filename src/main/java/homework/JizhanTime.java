package homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JizhanTime {
    public static class ForMap extends Mapper<LongWritable, Text, MRDPUtils, LongWritable> {
        MRDPUtils mrdpUtils ;
        LongWritable longWritable = new LongWritable();
        Date date;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            try {
                if (name.contains("pos")) {
                    date=simpleDateFormat.parse(strings[4]);
                    mrdpUtils=new MRDPUtils(strings[0],strings[3],date);
                }
                else if(name.contains("net")){
                    date=simpleDateFormat.parse(strings[3]);
                   mrdpUtils=new MRDPUtils(strings[0],strings[2],date);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if(mrdpUtils.getTimeDuring()==null){
                return;
            }
            longWritable.set(date.getTime());
            context.write(mrdpUtils,longWritable);
        }
    }

    public static class ForReducer extends Reducer<MRDPUtils,LongWritable,MRDPUtils,Text> {
        Text text=new Text();
        MRDPUtils tk=new MRDPUtils();
        Long min;
        Long max;
        @Override
        protected void reduce(MRDPUtils key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {


            for(LongWritable i:values){
                if(max==0||max<i.get()){
                    max=i.get();
                }
                if (min==0||min>i.get()){
                    min=i.get();
                }
            }
            tk=key;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int t=(int)(max-min)/1000/60;
            text.set(t+"分钟");
            context.write(tk,text);

        }
    }
}
