package homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommentWordCount {
    public static class ForMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        Text text=new Text();
        int t=0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               WeiboUtil weiboUtil = new WeiboUtil(value.toString());
                String txt=weiboUtil.getText();
                if(txt==null){
                    return;
                }
                 Pattern pattern=Pattern.compile("\\w+");
            Matcher matcher=pattern.matcher(txt);
            while(matcher.find()){
                text.set(matcher.group());
                context.write(text,NullWritable.get());
            }


        }
    }
    public static class ForReduce extends Reducer<Text,NullWritable,Text,IntWritable>{
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(NullWritable i:values){
                count++;
            }
            text.set(key);
            intWritable.set(count);
            context.write(text,intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(ForReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class9"));
        job.waitForCompletion(true);
    }
}
