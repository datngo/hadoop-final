/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */

package finalproject.problem3;

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
 * This Mapper read and return data for sorting times appearance descending.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperFourth extends
        Mapper<LongWritable, Text, IntWritable, Text> {
	
    /** 
     * IntWritable outputKey store times apperance of each code.
     * */
	private IntWritable outputKey = new IntWritable();
	/** 
     * Text outputValue store all various composite city and state code.
     * */
	private Text outputValue = new Text();

	/**
     * Return times appearance as Key and composite city and state code as Value.
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

        outputKey.set(Integer.parseInt(words[1].toString()));
        outputValue = new Text(words[0].toString());

        context.write(outputKey, outputValue);
    }
}