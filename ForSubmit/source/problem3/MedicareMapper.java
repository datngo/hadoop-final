/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Mapper read data input, group by Medicare code, city name and state name 
 * and return each Medicare code with a list of all corresponding bill Requests.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapper extends
        Mapper<LongWritable, Text, Text, DoubleWritable> {

    /** 
     * Text codeTreatment store all Medicare code
     * */
    private Text codeTreatment = new Text();
    /** 
     * Text cityInsurance store all city name of each Insurance
     * */
    private Text cityInsurance = new Text();
    /** 
     * Text stateInsurance store all state name of each city of each Insurance
     * */
    private Text stateInsurance = new Text();
    /** 
     * DoubleWritable billRequest store all corresponding bill request when grouping
     * */
    private DoubleWritable billRequest = new DoubleWritable();
    /** 
     * Text compositeKey store composite value of Medicare code, city name and state name for grouping
     * */
    private Text compositeKey = new Text();

    /**
     * Grouping medical care code, city name and state name.
     * Output a list of all corresponding bill request.
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
        cityInsurance = new Text(words[4]);
        stateInsurance = new Text(words[5]);
        
        billRequest = new DoubleWritable(Double.parseDouble(words[9].toString()));

        compositeKey = new Text(codeTreatment.toString() + "#" + cityInsurance.toString() + "#" + stateInsurance.toString());

        context.write(compositeKey, billRequest);
    }
}