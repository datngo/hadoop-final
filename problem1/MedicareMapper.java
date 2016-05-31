package finalproject.problem1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedicareMapper extends
        Mapper<LongWritable, Text, MedicareWritable, DoubleWritable> {

    private MedicareWritable wLog = new MedicareWritable();

    private Text codeTreatment = new Text();
    private DoubleWritable billRequest = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /***
         * sameple of weblog of RESOURCE: 127248 /rr.html 2014-03-10 12:32:08
         * 42.416.153.181
         */
        /***
         * sample of actual weblog 10.211.47.159 - - [29/Aug/2009:14:17:54
         * -0700] "GET /assets/img/search-button.gif HTTP/1.1" 200 168
         */
        // String[] words = value.toString().split("\t");
        // System.out.println("value.toString() == "+value.toString());
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
        billRequest = new DoubleWritable(Double.parseDouble(words[9]));

        wLog.set(codeTreatment);

        context.write(wLog, billRequest);
    }
}