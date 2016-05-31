package finalproject.problem1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapperSecond extends
        Mapper<LongWritable, Text, MedicareWritableSecond, DoubleWritable> {

    private MedicareWritableSecond wLog = new MedicareWritableSecond();

    private Text codeTreatment = new Text();
    private DoubleWritable billRequest = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String words[] = value.toString().split("\t");
        
        System.out.println("words[0] = " + words[0]);
        System.out.println("words[1] = " + words[1]);

        codeTreatment = new Text(words[0]);
        billRequest = new DoubleWritable(Double.parseDouble(words[1]));

        wLog.set(codeTreatment, billRequest);
        
        context.write(wLog, billRequest);
    }
    
//    public int compareTo(MedicareMapperSecond o){
//        return (billRequest.compareTo(o.billRequest));
//    }
}