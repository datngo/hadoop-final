package finalproject.problem2a;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    private Text codeTreatment = new Text();
    private Text codeInsurance = new Text();
    private Double billRequest = new Double(0.0);
    private Text compositeValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");
        System.out.println("words == " + words.length); // DEBUG show that it is
                                                        // 10
        System.out.println("words[0] == " + words[0]);
        System.out.println("words[1] == " + words[1]);
        System.out.println("words[2] == " + words[2]);
        System.out.println("words[3] == " + words[3]);
        System.out.println("words[4] == " + words[4]);
        System.out.println("words[5] == " + words[5]);
        System.out.println("words[6] == " + words[6]);
        System.out.println("words[7] == " + words[7]);
        System.out.println("words[8] == " + words[8]);
        System.out.println("words[9] == " + words[9]);
        System.out.println("words[10] == " + words[10]);

        codeTreatment = new Text(words[0]);
        codeInsurance = new Text(words[1]);
        
        billRequest = Double.parseDouble(words[9].toString());

        compositeValue = new Text(codeInsurance.toString() + "#" + billRequest.toString());

        context.write(codeTreatment, compositeValue);
    }
}