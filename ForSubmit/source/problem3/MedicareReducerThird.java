/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer count appearance times for each composite key city and state name
 * </pre>
 * 
 * </p>
 */
public class MedicareReducerThird extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    
    /**
     * IntWritable result store output value to write to output file
     * */
    private IntWritable result = new IntWritable();

    /**
     * Count appearance times of each composite code city and state name
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<DoubleWritable>
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
        result.set(count);
        context.write(key, result);
    }
}