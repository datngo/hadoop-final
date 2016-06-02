package finalproject.problem2a;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducerSecond extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Integer count = 0;

        for (@SuppressWarnings("unused")
        IntWritable val : values) {
            ++count;
        }
        result.set(count);

        context.write(key, result);
    }
}