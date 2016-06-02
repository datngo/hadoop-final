package finalproject.problem2a;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducer
        extends
        Reducer<Text, Text, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        double max = 0, billRequest = 0;
        String codeInsurance, outputKey = "NULL";
        String[] words;

        // compositeKey = key.getCompositeKey();
        System.out.println(key.toString());
        for (@SuppressWarnings("unused")
        Text val : values) {
            words = val.toString().split("#", 2);
            
            codeInsurance = words[0];
            billRequest = Double.parseDouble(words[1].toString());
            
            if (billRequest > max) {
                max = billRequest;
                outputKey = key + "#" + codeInsurance;
            }
        }
        result.set(max);
        context.write(new Text(outputKey), result);
    }
}