/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.io.IOException;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer calculate the appearance times of each Insurance code
 * </pre>
 * 
 * </p>
 * 
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducerSecond extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    
    /** 
     * IntWritable result store output value to write to output file 
     * */
    private final IntWritable result = new IntWritable();

    /**
     * Calculate the appearance times of each Insurance code
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<Text>
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Integer count = 0;

        for (@SuppressWarnings("unused")
        IntWritable val : values) {
           count++;
        }
        System.out.println("Count = " + count);
        result.set(count);

        context.write(key, result);
    }
}