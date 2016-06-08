/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Mapper 
 * output key is the appearance times for sorting descending
 * output value is the insurance code 
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperThird extends
        Mapper<LongWritable, Text, IntWritable, Text> {
    
    /** 
     * IntWritable outputKey store the appearance time for sorting.
     * */
    private IntWritable outputKey = new IntWritable();
    /** 
     * Text outputValue store all various Insurance code
     * */
    private Text outputValue = new Text();

    /**
     * Output the appearance times as key and insurance code as value
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Text
     * @param context
     *            Context
     * 
     */
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t", 2);
        
        outputKey = new IntWritable(Integer.parseInt(words[1].toString()));
        outputValue.set(words[0].toString());

        context.write(outputKey, outputValue);
    }
}