/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * <p>
 * 
 * <pre>
 * This comparable sort value of Doublewritable descending
 * </pre>
 * 
 * </p>
 * 
 */
public class DescendingDoubleWritableComparable extends WritableComparator{
    protected DescendingDoubleWritableComparable(){
        super(DoubleWritable.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
    	DoubleWritable k1 = (DoubleWritable) w1;
    	DoubleWritable k2 = (DoubleWritable) w2;
        
        return (-1) * k1.compareTo(k2);
    }
}