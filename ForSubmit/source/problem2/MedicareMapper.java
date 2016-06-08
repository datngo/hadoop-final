/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Mapper read data input, group by Medicare code 
 * and return each Medicare code with corresponding composite value.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    /** 
     * Text codeTreatment store all various Medicare code for grouping.
     * */
    private Text codeTreatment = new Text();
    /** 
     * Text codeTreatment store all corresponding Insurance code when grouping.
     * */
    private Text codeInsurance = new Text();
    /** 
     * Double billRequest store all corresponding bill request when grouping.
     * */
    private Double billRequest = new Double(0.0);
    /**
     * Text compositeValue store Insurance code and bill request, can be distinguished by "#" 
     * */
    private Text compositeValue = new Text();

    /**
     * Grouping each medical care code with a list of all corresponding composite value.
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

        String[] words = value.toString().split("\t");

        codeTreatment = new Text(words[0]);
        codeInsurance = new Text(words[1]);
        
        billRequest = Double.parseDouble(words[9].toString());

        compositeValue = new Text(codeInsurance.toString() + "#" + billRequest.toString());

        context.write(codeTreatment, compositeValue);
    }
}