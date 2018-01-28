package homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MaxMinSimple {
    public static class ForMap extends Mapper<LongWritable,Text,Text,DateTimeUtil>{
        Text text=new Text();
        DateTimeUtil dateTimeUtil=new DateTimeUtil();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            WeiboUtil weiboUtil = new WeiboUtil(value.toString());
            SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            try {
                if(weiboUtil.getTime()==null || weiboUtil.getUserId()==null){
                    return;
                }
                Date time=s.parse(weiboUtil.getTime());
                System.out.println(time);
                text.set(weiboUtil.getUserId());
                dateTimeUtil.setMax(time);
                dateTimeUtil.setMin(time);
                context.write(text,dateTimeUtil);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
    public static class ForReduce extends Reducer<Text,DateTimeUtil,Text,DateTimeUtil>{
        Text text=new Text();
        DateTimeUtil dateTimeUtil=new DateTimeUtil();
        @Override
        protected void reduce(Text key, Iterable<DateTimeUtil> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            dateTimeUtil.setMin(null);
            dateTimeUtil.setMax(null);
            for(DateTimeUtil d:values){
                sum++;
                if(dateTimeUtil.getMin()==null || d.getMin().compareTo(dateTimeUtil.getMin())<0){
                    dateTimeUtil.setMin(d.getMin());

                }
                if(dateTimeUtil.getMax()==null || d.getMax().compareTo(dateTimeUtil.getMax())>0){
                    dateTimeUtil.setMax(d.getMax());
                }
            }
            dateTimeUtil.setCount(sum);
            text.set(key);
            context.write(text,dateTimeUtil);

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DateTimeUtil.class);
        job.setReducerClass(ForReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DateTimeUtil.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class23"));
        job.waitForCompletion(true);
    }
}
