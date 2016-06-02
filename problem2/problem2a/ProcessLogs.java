package finalproject.problem2a;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessLogs {

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {// Remember to config argument
            System.out
                    .printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2 - input 3 dir><output 3>\n");
            System.exit(-1);
        }

        /*
         * Job 1 Return list code Treatment with insurance that request the
         * highest bill
         */
        Job job = new Job();
        job.setJarByClass(ProcessLogs.class);
        job.setJobName("Process Logs 1");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedicareMapper.class); // Set mapper
        job.setReducerClass(MedicareReducer.class); // Set reducer

        job.setMapOutputKeyClass(Text.class); // Set mapper output key
        job.setMapOutputValueClass(Text.class); // Set mapper output value

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        boolean success = job.waitForCompletion(true);

        /*
         * Job 2 Return code Insurance with number of times appearance
         */
        if (success) {
            Job jobSecond = new Job();

            jobSecond.setJobName("Count times of Insurances'appearance");
            jobSecond.setJarByClass(ProcessLogs.class);

            FileInputFormat.setInputPaths(jobSecond, new Path(args[1] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));

            jobSecond.setMapperClass(MedicareMapperSecond.class);
            jobSecond.setReducerClass(MedicareReducerSecond.class);

            jobSecond.setMapOutputKeyClass(Text.class);
            jobSecond.setMapOutputValueClass(IntWritable.class);

            jobSecond.setOutputKeyClass(Text.class);
            jobSecond.setOutputValueClass(IntWritable.class);

            success = jobSecond.waitForCompletion(true);

        } else {
            System.exit(-1);
        }

        /*
         * Job 3 Sorting value and return top 3
         */

        if (success) {
            Job jobThird = new Job();

            jobThird.setJobName("Sort times Insurances' appearance");
            jobThird.setJarByClass(ProcessLogs.class);

            // FileInputFormat.setInputPaths(jobSecond, new Path(args[1]) +
            // "/part-r-00000");
            FileInputFormat.setInputPaths(jobThird, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobThird, new Path(args[3]));

            // Sort descending key
            jobThird.setSortComparatorClass(DescendingIntWritableComparable.class);

            jobThird.setMapperClass(MedicareMapperThird.class);
            jobThird.setReducerClass(MedicareReducerThird.class);

            jobThird.setMapOutputKeyClass(IntWritable.class);
            jobThird.setMapOutputValueClass(Text.class);

            jobThird.setOutputKeyClass(Text.class);
            jobThird.setOutputValueClass(IntWritable.class);

            success = jobThird.waitForCompletion(true);

            System.exit(success ? 0 : -1);

        } else {
            System.exit(-1);
        }
    }
}
