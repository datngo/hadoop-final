/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

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
 * This Mapper read data input, group by Insurance code 
 * and return Insurance code with value 1.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    
    /** 
     * Text codeTreatment store all various Insurance code for grouping.
     * */
    private Text codeInsurance = new Text();

    /** 
     * IntWritable time has fixed value 1.
     * */
    private final IntWritable time = new IntWritable(1);

    /**
     * Grouping each Insurance code with a list of all appearances have value 1.
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

        codeInsurance = new Text(words[0].toString().split("#", 2)[1]);

        context.write(codeInsurance, time);
    }
}