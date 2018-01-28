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

public class MRsourcecount {
    public static class ForMap extends Mapper<LongWritable,Text,Text,Text>{
        Text text=new Text();
        Text t=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(" ");
            text.set(strings[0]);
            t.set(strings[10]);
            context.write(text,t);
        }
    }
    public static class ForReduce extends Reducer<Text,Text,Text,Text>{
        Text text=new Text();
        Text tv=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text i:values){
                if(!i.toString().equals("\"-\"")){
                    sb.append("\t"+i.toString());

                }

            }
            text.set(key);
            tv.set(sb.toString());
            if(!sb.toString().equals("")){
                context.write(text,tv);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        //job.setJarByClass(TotalScoreMR.class);

        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ForReduce.class);
        // job.setOutputKeyClass(KPIBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce经典案例 WordCount 练习题及答案\\实验数据"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class14"));
        job.waitForCompletion(true);
    }
}
