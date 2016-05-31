package finalproject.problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducerSecond extends
        Reducer<MedicareWritableSecond, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private Text codeTreatment = new Text();
    private static Integer count = 0;

    public void reduce(MedicareWritableSecond key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
        double relativeVariance = 0;

        ++count;
        System.out.println("count = " + count);
        if(count < 4){
            codeTreatment = key.getCodeTreatment();
            for (@SuppressWarnings("unused")
            DoubleWritable val : values) {
                relativeVariance = val.get();
            }
            result.set(relativeVariance);

            context.write(codeTreatment, result);

        }
    }
}