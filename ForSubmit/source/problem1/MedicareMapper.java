/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

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
 * This Mapper read data input, group by Medicare code 
 * and return each Medicare code with all corresponding bill Requests.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapper extends
        Mapper<LongWritable, Text, Text, DoubleWritable> {

    /** 
     * Text codeTreatment store all various Medicare code for grouping.
     * */
    private Text codeTreatment = new Text();
    /** 
     * DoubleWritable billRequest store all bill requests corresponding each code when grouping. 
     * */
    private DoubleWritable billRequest = new DoubleWritable();

    /**
     * Grouping each medical care code with a list of all corresponding bill request.
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

        codeTreatment = new Text(words[0].toString());
        billRequest = new DoubleWritable(Double.parseDouble(words[9].toString()));

        context.write(codeTreatment, billRequest);
    }
}