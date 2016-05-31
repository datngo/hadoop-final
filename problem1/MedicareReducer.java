package finalproject.problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedicareReducer extends
        Reducer<MedicareWritable, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private Text codeTreatment = new Text();
    private List<DoubleWritable> cache = new ArrayList<DoubleWritable>();

    public void reduce(MedicareWritable key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
        double sum = 0, relativeVariance = 0, mean = 0, variance = 0;
        int length = 0;

        codeTreatment = key.getCodeTreatment();

        for (@SuppressWarnings("unused")
        DoubleWritable val : values) {
            cache.add(val);
            sum += val.get();
            length++;
        }
        System.out.println("SUM: " + sum);
        System.out.println("length: " + length);
        mean = sum / length;
        System.out.println("mean: " + mean);
        sum = 0;
        for (@SuppressWarnings("unused")
        DoubleWritable val : cache) {
            double tmp = (val.get() - mean);
            System.out.println("tmp = " + tmp);
            double tmp1 = Math.pow(tmp, 2);
            System.out.println("tmp1 = " + tmp1);
            sum += tmp1;
        }
        System.out.println("SUM 2:: " + sum);
        variance = sum / length;
        System.out.println("variance: " + variance);
        relativeVariance = variance / Math.pow(mean, 2);

        result.set(relativeVariance);
        context.write(codeTreatment, result);
    }
}