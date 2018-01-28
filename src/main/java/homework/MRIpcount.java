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

public class MRIpcount {
    public static class ForMap extends Mapper<LongWritable,Text,KPIBean,Text>{
        KPIBean kpiBean=new KPIBean();
        Text text=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(" ");
            kpiBean.setBrowser(strings[6]);
            kpiBean.setIps(strings[0]);
            text.set("0");
            context.write(kpiBean,text);
        }
    }
    public static class ForComp extends Reducer<KPIBean,Text,KPIBean,Text>{
        KPIBean k=new KPIBean();
        Text text=new Text();

        @Override
        protected void reduce(KPIBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(Text t:values){
                count++;
            }
            k.setBrowser(key.getBrowser());
            text.set(key.getIps()+"---"+count);
            context.write(k,text);

        }
    }
    public static class ForReduce extends Reducer<KPIBean,Text,Text,NullWritable>{
        Text text=new Text();
        @Override
        protected void reduce(KPIBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            sb.append(key.getBrowser());
            for(Text t:values){
                sb.append("\t"+t.toString());
            }
            sb.append("；");
            text.set(sb.toString());
            context.write(text,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job=Job.getInstance();
        //job.setJarByClass(TotalScoreMR.class);
        job.setCombinerClass(ForComp.class);
        job.setMapperClass(ForMap.class);
        job.setMapOutputKeyClass(KPIBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ForReduce.class);
       // job.setOutputKeyClass(KPIBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\甲骨文\\MapReduce经典案例 WordCount 练习题及答案\\实验数据"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\class8"));
        job.waitForCompletion(true);
    }


}
