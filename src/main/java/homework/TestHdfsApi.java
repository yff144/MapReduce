package homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestHdfsApi {
    private static String url="hdfs://192.168.227.132:8020";
    private static FileSystem fs;
    static {
        try {
             fs=FileSystem.get(new URI(url),new Configuration(),"groot");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    public void up (String s1,String s2) throws IOException {
        fs.copyFromLocalFile(new Path(s1),new Path(s2));
    }
    public void down(String s1,String s2) throws IOException {
        fs.copyToLocalFile(new Path(s1),new Path(s2));
    }
    public void mkd(String path) throws IOException {
        FSDataOutputStream fsDataOutputStream=fs.create(new Path(path));
        for(int i=0;i<10;i++){
            fsDataOutputStream.write(("haha"+i).getBytes());
            fsDataOutputStream.flush();
        }
        fsDataOutputStream.close();
    }
    public void mkdir(String path) throws IOException {
        fs.mkdirs(new Path(path));
    }
    public  void rename(String path1,String path2) throws IOException {
        fs.rename(new Path(path1),new Path(path2));
    }
    public void delete(String path) throws IOException {
        fs.delete(new Path(path),false);
    }
    public void deletedir(String path) throws IOException {
        fs.delete(new Path(path),true);
    }
    public boolean see(String path) throws IOException {
        return fs.isFile(new Path(path));
    }
    public long time(String path) throws IOException {
        FileStatus fileStatus=fs.getFileStatus(new Path(path));
        long time=fileStatus.getAccessTime();
        return time;
    }
    public void read(String path) throws IOException {
        if(fs.isDirectory(new Path(path))){

        }
    }


}
