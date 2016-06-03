package finalproject.problem3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingIntWritableComparable extends WritableComparator{
    protected DescendingIntWritableComparable(){
        super(IntWritable.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        IntWritable k1 = (IntWritable) w1;
        IntWritable k2 = (IntWritable) w2;
        
        return (-1) * k1.compareTo(k2);
    }
}