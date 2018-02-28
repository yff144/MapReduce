package for0123;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.MappedByteBuffer;

@SuppressWarnings("ALL")
public class TotalScoreMR {
    public static class ForMap extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(" ");
            text.set(strings[0]);
            intWritable.set(Integer.parseInt(strings[1]));
            System.out.println("hahahaha");
            context.write(text,intWritable);
        }
    }
    public static class ForReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            String []s=key.toString().split("");
            text.set(s[0]+"年级");
            intWritable.set(sum);
            context.write(text,intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job=Job.getInstance();
        job.setJarByClass(TotalScoreMR.class);
        job.setGroupingComparatorClass(MyClassTextComparator.class);
        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ForReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        job.waitForCompletion(true);
    }
}
