package finalproject.problem2a;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducerThird extends
        Reducer<IntWritable, Text, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private Text codeInsurance = new Text();
    private static Integer count = 0;

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ++count;
        if (count < 4) {
            for (@SuppressWarnings("unused")
            Text val : values) {
                codeInsurance = new Text(val.toString());
                result.set(key.get());
                context.write(new Text(codeInsurance), result);
            }
        }
    }
}