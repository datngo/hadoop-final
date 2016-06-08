/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer return maximum of bill request of each Medicare code with Insurance code
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    
    /** 
     * DoubleWritable result store maximum of bill request of each Medicare code.
     * */
    private DoubleWritable result = new DoubleWritable();

    /**
     * Return code of Medicare with code of Insurance as output Key, can be distinguished by "#"
     *  and maximum of bill request as output Value
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Text
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = 0, billRequest = 0;
        String codeInsurance, outputKey = "NULL";
        String[] words;

        for (Text val : values) {
            words = val.toString().split("#", 2);

            codeInsurance = words[0];
            billRequest = Double.parseDouble(words[1].toString());

            if (billRequest > max) {
                max = billRequest;
                outputKey = key + "#" + codeInsurance;
            }
        }
        result.set(max);
        context.write(new Text(outputKey), result);
    }
}