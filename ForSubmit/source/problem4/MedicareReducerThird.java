/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer output top 3 of descending list of Insurance code after sorting  by the appearance times
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducerThird extends
		Reducer<IntWritable, Text, Text, NullWritable> {

    /** 
     * Text outputKey store output key to write to output file 
     * */
	private Text outputKey = new Text();
	
	/** 
     * Integer count is auto-increase and this program will end when it equals 3. 
     * */
	private static Integer count = 0;

	/**
     * Return top 3 of maximum appearance times of Insurance code after sorting
     * 
     * @param key
     *            LongWritable
     * @param values
     *            Iterable<Text>
     * @param context
     *            Context
     * 
     */
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		++count;
		if (count < 4) {
			for (
			Text val : values) {
				outputKey = new Text(val.toString());
			}
			context.write(outputKey, NullWritable.get());

		}

	}
}