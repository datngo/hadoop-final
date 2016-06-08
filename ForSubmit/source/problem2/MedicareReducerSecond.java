/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer return appearance times each Insurance code when grouping
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducerSecond extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    
    /** 
     * IntWritable result store appearance times of each Insurance code.
     * */
    private IntWritable result = new IntWritable();

    /**
     * Return code of Insurance as output Key
     *  and times of appearance as output Value
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Text
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Integer count = 0;

        for (@SuppressWarnings("unused")
        IntWritable val : values) {
            ++count;
        }
        result.set(count);

        context.write(key, result);
    }
}