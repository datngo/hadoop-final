package finalproject.problem2a;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable time = new IntWritable(1);
    private Text codeInsurance = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t", 2);
        
        System.out.println("words[0] = " + words[0]);
        System.out.println("words[1] = " + words[1]);

        codeInsurance = new Text(words[0].toString().split("#", 2)[1]);

        
        context.write(codeInsurance, time);
    }
}