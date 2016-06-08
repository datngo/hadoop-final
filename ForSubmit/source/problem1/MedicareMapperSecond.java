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
 * This Mapper read and return data for sorting relative variance descending.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, DoubleWritable, Text> {

    /** 
     * Text codeTreatment store all various Medicare code.
     * */
    private Text codeTreatment = new Text();
    
    /** 
     * DoubleWritable relativeVariance store corresponding relative variance to each code.
     * */
    private DoubleWritable relativeVariance = new DoubleWritable();

    /**
     * Return relative variance as Key and Medicare code as Value.
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

        String words[] = value.toString().split("\t");

        codeTreatment = new Text(words[0]);
        relativeVariance = new DoubleWritable(Double.parseDouble(words[1]));

        context.write(relativeVariance, codeTreatment);
    }
}