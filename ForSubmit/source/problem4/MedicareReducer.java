/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer calculate maximum value of bill gap for each medicare code
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    
    /** 
     * DoubleWritable result store output value to write to output file 
     * */
    private DoubleWritable result = new DoubleWritable();
    
    /** 
     * Text outputKey store output key to write to output file 
     * */
    private Text outputKey = new Text();

    /**
     * Calculate maximum value of bill gap for each Medicare code
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<Text>
     * @param context
     *            Context
     * 
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = 0;

        for (
        Text val : values) {
        	String words[] = val.toString().split("#", 2);
        	double billGap = Double.parseDouble(words[1].toString());
            if(billGap > max){
            	max = billGap;
            	outputKey = new Text(words[0].toString());
            }
        }
        
        result.set(max);
        context.write(outputKey, result);
    }
}