/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer return top 3 of Insurance after sorting.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducerThird extends
        Reducer<IntWritable, Text, Text, NullWritable> {
    
    /** 
     * IntWritable result store corresponding appearance times to each code.
     * */
    private IntWritable result = new IntWritable();
    
    /** 
     * Text codeInsurance store Insurance code.
     * */
    private Text codeInsurance = new Text();

    /** 
     * Integer count is auto-increase and this program will end when it equals 3. 
     * */
    private static Integer count = 0;
    
    /**
     * Return top 3 Insurance code with appearance times
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Text
     * @param context
     *            Context
     * 
     */
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ++count;
        if (count < 4) {
            for (
            Text val : values) {
                codeInsurance = new Text(val.toString());
                result.set(key.get());
                context.write(new Text(codeInsurance), NullWritable.get());
            }
        }
    }
}