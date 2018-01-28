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

public class AverageDriver {
    public static class ForMap extends Mapper<LongWritable,Text,WeiboUtil,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            WeiboUtil weiboUti=new WeiboUtil(value.toString());
            if(weiboUti.getTime().equals("")){
                return;
            }
            context.write(weiboUti,NullWritable.get());
        }
    }
    public static class ForReduce extends Reducer<WeiboUtil,NullWritable,Text,Text>{
        Text textk=new Text();
        Text textv=new Text();
        int sum=0;
        @Override
        protected void reduce(WeiboUtil key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Pattern pattern=Pattern.compile("\\w");
            Matcher matcher=pattern.matcher(key.getText());
            while (matcher.find()){
                sum++;
            }
            int count=0;
            for(NullWritable i:values){
                count++;
            }
            String s=key.getTime().substring(8,10)+"日"+key.getTime().substring(11,13)+"时";
            textk.set(s);
            int avg=sum/count;
            textv.set(count+"条微博,平均字数"+avg);
            context.write(textk,textv);


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(WeiboUtil.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(ForReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class33"));
        job.waitForCompletion(true);
    }

}
