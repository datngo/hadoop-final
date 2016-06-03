package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapperThird extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private Text compositeKey = new Text();
    private final IntWritable count = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t", 2);
        
        System.out.println("words[0] = " + words[0]);
        System.out.println("words[1] = " + words[1]);

        compositeKey = new Text(words[0].toString());

        context.write(compositeKey, count);
    }
}