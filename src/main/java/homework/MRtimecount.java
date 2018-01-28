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

public class MRtimecount {
    public static class ForMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        Text text=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []string=value.toString().split(" ");
            text.set(string[3]);
            context.write(text,NullWritable.get());

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
            String s=key.toString().substring(19,21);
            text.set(s+"秒");
            intWritable.set(count);
            context.write(text,intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Job job=Job.getInstance();
//        //job.setJarByClass(TotalScoreMR.class);
//        job.setGroupingComparatorClass(KPIBean.class);
//        job.setMapperClass(ForMap.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setReducerClass(ForReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce经典案例 WordCount 练习题及答案\\实验数据"));
//        FileOutputFormat.setOutputPath(job,new Path("D:\\class15"));
//        job.waitForCompletion(true);
        String s="  <";
        System.out.println(s.length());

    }

}
