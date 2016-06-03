package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducerSecond extends
        Reducer<Text, Text, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private Text outputKey = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = 0;

        for (@SuppressWarnings("unused")
        Text val : values) {
            String words[] = val.toString().split("#", 3);
            double averageBillRequest = Double.parseDouble(words[2].toString());
            if (averageBillRequest > max) {
                max = averageBillRequest;
                outputKey = new Text(words[0] + "#" + words[1]);
            }
        }
        result.set(max);

        context.write(outputKey, result);
    }
}