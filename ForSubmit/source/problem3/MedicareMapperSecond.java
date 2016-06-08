/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */

package finalproject.problem3;

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
 * and return each Medicare code with a list of all corresponding average bill Requests.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, Text, Text> {
    
    /** 
     * Text codeTreatment store all Medicare code
     * */
    private Text codeTreatment = new Text();
    
    /** 
     * Text cityInsurance store city name of Insurance
     * */
    private Text cityInsurance = new Text();
    
    /** 
     * Text stateInsurance store state name of Insurance
     * */
    private Text stateInsurance = new Text();
    
    /** 
     * Double billRequest store all corresponding bill requests
     * */
    private Double billRequest = new Double(0);

    /** 
     * Text compositeValue store value that be combined by city name, state name and bill request
     * */
    private Text compositeValue = new Text();

    
    /**
     * Grouping medical care code.
     * Output a list of all corresponding composite value.
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

        codeTreatment = new Text(words[0].toString().split("#", 3)[0]);
        cityInsurance = new Text(words[0].toString().split("#", 3)[1]);
        stateInsurance = new Text(words[0].toString().split("#", 3)[2]);
        billRequest = Double.parseDouble(words[1].toString());

        compositeValue = new Text(cityInsurance + "#" + stateInsurance + "#"
                + billRequest);

        context.write(codeTreatment, compositeValue);
    }
}