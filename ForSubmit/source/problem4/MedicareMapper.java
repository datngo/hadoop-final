/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

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
 * and return each Medicare code with all corresponding composite value (code insurance and bill gap)
 * bill gap: different between bill request and bill actual.
 * </pre>
 * 
 * </p>
 * 
 */
public class MedicareMapper extends Mapper<LongWritable, Text, Text, Text> {

    /** 
     * Text codeTreatment store all various Medicare code for grouping.
     * */
	private Text codeTreatment = new Text();

    /** 
     * Text codeInsurance store all code of Insurance
     * */
	private Text codeInsurance = new Text();

    /** 
     * Text compositeValue is combined by codeInsurance and billGap
     * */
	private Text compositeValue = new Text();

    /** 
     * DoubleWritable billGap - different between bill request and bill actual
     * */
	private DoubleWritable billGap = new DoubleWritable();

	/**
     * Grouping each medical care code with a list of all corresponding composite value
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
		double billRequest, billActual;

		codeTreatment = new Text(words[0].toString());
		codeInsurance = new Text(words[1].toString());

		billRequest = Double.parseDouble(words[9].toString());
		billActual = Double.parseDouble(words[10].toString());

		billGap = new DoubleWritable(Math.abs(billRequest - billActual));
		System.out.println("billGap = " + billGap);

		compositeValue = new Text(codeInsurance + "#" + billGap);

		context.write(codeTreatment, compositeValue);
	}
}