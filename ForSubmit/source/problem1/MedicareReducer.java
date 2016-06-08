/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <p>
 * 
 * <pre>
 * This Reducer calculate relative variance value for each medicare code
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareReducer extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	/** 
	 * Result store output value to write to output file 
	 * */
	private DoubleWritable result = new DoubleWritable();

	/**
	 * Calculate relative variance for each medical care code
	 * 
	 * @param key
	 *            LongWritable
	 * @param values
	 *            Iterable<DoubleWritable>
	 * @param context
	 *            Context
	 * 
	 */
	public void reduce(Text key, Iterable<DoubleWritable> values,
	        Context context) throws IOException, InterruptedException {
		List<Double> cacheDouble = new ArrayList<Double>();

		double sum = 0, mean = 0, variance = 0, relativeVariance = 0;
		int length = 0;

		for (DoubleWritable val : values) {
			length = length + 1;
			sum = sum + val.get();
			cacheDouble.add(new Double(val.get()));
		}

		mean = sum / length;
		float fMean = (float) mean;
		double total = 0;
		for (Double mval : cacheDouble) {
			float tmp = ((float) mval.doubleValue() - fMean);
			double tmp1 = (double) (tmp * tmp);

			total = total + tmp1;
		}

		variance = total / length;

		relativeVariance = variance / (mean * mean);

		result.set(relativeVariance);
		context.write(key, result);
	}
}