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
 * This Mapper read and return data for sorting times of appearance.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperThird extends
        Mapper<LongWritable, Text, IntWritable, Text> {
    
    /** 
     * Text codeTreatment store all various Insurance code.
     * */
    private Text codeInsurance = new Text();
    /** 
     * IntWritable count store all appearance times of corresponding insurance code
     * */
    private IntWritable count = new IntWritable();

    /**
     * Return count as output Key and codeInsurance as output Value.
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

        codeInsurance = new Text(words[0].toString());
        count = new IntWritable(Integer.parseInt(words[1].toString()));

        context.write(count, codeInsurance);
    }
}