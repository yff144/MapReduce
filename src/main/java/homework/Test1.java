package homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.util.List;

public class Test1 {
    public static class ForMap extends Mapper<Text,Text,Text,NullWritable>{
        Text text=new Text();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            System.out.println(s+"---------------------");
            List<Word> list=WordSegmenter.segWithStopWords(s);
            System.out.println(list+"===============");
            for(Word i:list){

                text.set(i.toString());
                context.write(text,NullWritable.get());
            }
        }
    }
    public static class ForReduce extends Reducer<Text,NullWritable,Text,IntWritable>{
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(NullWritable i:values){
                count++;
            }
            intWritable.set(count);
            context.write(key,intWritable);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setInputFormatClass(InputFilesFormatd.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(ForReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\a"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class13"));
        job.waitForCompletion(true);
    }

}
