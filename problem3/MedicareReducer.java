package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double average = 0, sum = 0;
        Integer count = 0;

        for (@SuppressWarnings("unused")
        DoubleWritable val : values) {
            sum += val.get();
            count += 1;
        }
        average = sum/count;
        result.set(average);
        context.write(new Text(key), result);
    }
}