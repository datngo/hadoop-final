package finalproject.problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, Text, Text> {
    private Text codeTreatment = new Text();
    private Text cityInsurance = new Text();
    private Text stateInsurance = new Text();
    private Double billRequest = new Double(0);
    
    private Text compositeValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t", 2);
        
        System.out.println("words[0] = " + words[0]);
        System.out.println("words[1] = " + words[1]);

        codeTreatment = new Text(words[0].toString().split("#", 3)[0]);
        cityInsurance = new Text(words[0].toString().split("#", 3)[1]);
        stateInsurance = new Text(words[0].toString().split("#", 3)[2]);
        billRequest = Double.parseDouble(words[1].toString());
        
        compositeValue = new Text(cityInsurance + "#" + stateInsurance + "#" + billRequest);

        
        context.write(codeTreatment, compositeValue);
    }
}