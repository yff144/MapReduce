package homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount {
    public static class ForMap extends Mapper<LongWritable,Text,KPIBean,IntWritable>{
        KPIBean kpiBean=new KPIBean();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(" - - ");
            kpiBean.setBrowser(strings[0]);


        }
    }
    public static class ForReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable i:values){
                count++;
            }
            text.set(key);
            intWritable.set(count);
            context.write(text,intWritable);
        }
    }

    public static void main(String[] args) {

    }

}
