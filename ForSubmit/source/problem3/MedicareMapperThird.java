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
 * This Mapper read data input, group by composite key from output of Job 2
 * and return 1 for each corresponding composite key.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapperThird extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    
    /** 
     * Text compositeKey store all composite key (city name + state name)
     * */
    private Text compositeKey = new Text();
    
    /** 
     * IntWritable count equals 1 for each composite key.
     * */
    private final IntWritable count = new IntWritable(1);

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
        
        String words[] = value.toString().split("\t", 2);

        compositeKey = new Text(words[0].toString());

        context.write(compositeKey, count);
    }
}