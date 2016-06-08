/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer calculate average bill request value for each medicare code with city and state name
 * </pre>
 * 
 * </p>
 */
public class MedicareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    /** 
     * Result store output value to write to output file 
     * */
    private DoubleWritable result = new DoubleWritable();

    
    /**
     * Calculate average bill request for each composite key.
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<DoubleWritable>
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double average = 0, sum = 0;
        Integer count = 0;

        for (
        DoubleWritable val : values) {
            sum += val.get();
            count += 1;
        }
        average = sum/count;
        result.set(average);
        context.write(new Text(key), result);
    }
}