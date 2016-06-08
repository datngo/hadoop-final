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
 * This Reducer calculate the maximum of average bill request for each composite key
 * </pre>
 * 
 * </p>
 */
public class MedicareReducerSecond extends
        Reducer<Text, Text, Text, DoubleWritable> {
    
    /**
     * DoubleWritable result store output value to write to output file
     * */
    private final DoubleWritable result = new DoubleWritable();
    
    /**
     * Text outputKey store output key to write to output file
     * */
    private Text outputKey = new Text();

    /**
     * Calculate maximum of average bill request for each composite key.
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<DoubleWritable>
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double maxAverage = 0;

        for (
        Text val : values) {
            String words[] = val.toString().split("#", 3);
            double averageBillRequest = Double.parseDouble(words[2].toString());
            if (averageBillRequest > maxAverage) {
            	maxAverage = averageBillRequest;
                outputKey = new Text(words[0] + "#" + words[1]);
            }
        }
        System.out.println("Max average = " + maxAverage);
        result.set(maxAverage);

        context.write(outputKey, result);
    }
}