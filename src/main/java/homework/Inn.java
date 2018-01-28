package homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class Inn extends RecordReader<Text,Text> {
    private Text key=new Text();
    private Text value=new Text();
    private boolean isFinish;
    private CombineFileSplit combineFileSplit;
    private FSDataInputStream inputStream;
    private TaskAttemptContext context;
    private Integer currentIndex;

    public Inn(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer currentIndex) {
        this.combineFileSplit = combineFileSplit;
        this.context = context;
        this.currentIndex = currentIndex;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isFinish){
            Path path=combineFileSplit.getPath(currentIndex);
            String name=path.getName();
            key.set(name);
            FileSystem fileSystem=path.getFileSystem(new Configuration());
            inputStream=fileSystem.open(path);
            byte []arr=new byte[(int)combineFileSplit.getLength(currentIndex)];
            inputStream.readFully(arr);
            value.set(arr.toString());
            isFinish=true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isFinish?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}
