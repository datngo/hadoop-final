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
 * This Mapper read data input, group by Insurance code 
 * and output each Insurance code with all corresponding bill gap
 * bill gap: different between bill request and bill actual.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    
    /** 
     * Text codeInsurance store all various Insurance code for grouping.
     * */
    private Text codeInsurance = new Text();
    
    /** 
     * IntWritable result equals 1 to count each insurance code.
     * */
    private final IntWritable result = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t", 2);

        codeInsurance = new Text(words[0].toString());

        context.write(codeInsurance, result);
    }
}