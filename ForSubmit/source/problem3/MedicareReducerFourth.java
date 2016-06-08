/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

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
 * This Reducer sort times appearance descending and output top 3 of list.
 * </pre>
 * 
 * </p>
 */
public class MedicareReducerFourth extends
        Reducer<IntWritable, Text, Text, NullWritable> {

    /**
     * IntWritable result store output value to write to output file
     * */
    private IntWritable result = new IntWritable();

    /**
     * Integer count is auto-increase and this program will end when it equals
     * 3.
     * */
    private static Integer count = 0;

    /**
     * Output top 3 of descending times appearance of each composite key city and state
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<DoubleWritable>
     * @param context
     *            Context
     * 
     */
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ++count;

        if (count < 4) {
            String outputKey = "";
            for (Text val : values) {
                outputKey = val.toString();
            }
            result.set(Integer.parseInt(key.toString()));
            context.write(new Text(outputKey), NullWritable.get());
        }
    }
}