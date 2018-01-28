package for0123;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyClassTextComparator  extends WritableComparator {
    public MyClassTextComparator() {
        super(Text.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        System.out.println(a.toString().substring(0,1)+"=========================");
        System.out.println(b.toString().substring(0,1)+"===========================");
        return a.toString().substring(0,1).compareTo(b.toString().substring(0,1));
    }
}
