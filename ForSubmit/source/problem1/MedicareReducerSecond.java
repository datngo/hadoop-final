/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer returns only top 3 elements after sorting as final output for this program.
 * </pre>
 * </p>
 * 
 */
public class MedicareReducerSecond extends
        Reducer<DoubleWritable, Text, Text, NullWritable> {
    
    /** 
     * Text codeTreatment store Medicare code.
     * */
    private Text codeTreatment = new Text();
    
    /** 
     * DoubleWritable relativeVariance store corresponding relative variance to each code.
     * */
    private DoubleWritable result = new DoubleWritable();
    
    /** 
     * Integer count is auto-increase and this program will end when it equals 3. 
     * */
    private static Integer count = 0;

    /**
     * Return top 3 Medicare code with relative variance
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Text
     * @param context
     *            Context
     * 
     */
    public void reduce(DoubleWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

        double relativeVariance = 0;

        ++count;

        if(count < 4){
        	relativeVariance = key.get();
            for (Text val : values) {
                codeTreatment = new Text(val.toString());
            }
            result.set(relativeVariance);

            context.write(codeTreatment, NullWritable.get());

        }
    }
}