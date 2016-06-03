package finalproject.problem3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessLogs {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {// Remember to config argument
            System.out
                    .printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2 - input 3 dir><output 3>\n");
            System.exit(-1);
        }

        /*
         * Job 1 Return list (k, v) grouping by k
         * k = codeTreatment#cityInsurance#stateInsurance
         * v = average of all billRequest 
         */
//        Job job = new Job();
//        job.setJarByClass(ProcessLogs.class);
//        job.setJobName("Process Logs 1");
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setMapperClass(MedicareMapper.class); // Set mapper
//        job.setReducerClass(MedicareReducer.class); // Set reducer
//
//        job.setMapOutputKeyClass(Text.class); // Set mapper output key
//        job.setMapOutputValueClass(DoubleWritable.class); // Set mapper output value
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
//
//        boolean success = job.waitForCompletion(true);
        boolean success = true;

        /*
         * Job 2: return (k, v)
         * k = cityInsurance#stateInsurance
         * v = maximum of average of all billRequest
         */
        if (success) {
//            Job jobSecond = new Job();
//
//            jobSecond.setJobName("List of geo-locations have the highest average billRequest");
//            jobSecond.setJarByClass(ProcessLogs.class);
//
//            //FileInputFormat.setInputPaths(jobSecond, new Path(args[1] + "/part-r-00000"));
//            //FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));
//            FileInputFormat.setInputPaths(jobSecond, new Path(args[0]));
//            FileOutputFormat.setOutputPath(jobSecond, new Path(args[1]));
//
//            jobSecond.setMapperClass(MedicareMapperSecond.class);
//            jobSecond.setReducerClass(MedicareReducerSecond.class);
//
//            jobSecond.setMapOutputKeyClass(Text.class);
//            jobSecond.setMapOutputValueClass(Text.class);
//
//            jobSecond.setOutputKeyClass(Text.class);
//            jobSecond.setOutputValueClass(Double.class);
//
//            success = jobSecond.waitForCompletion(true);

        } else {
            System.exit(-1);
        }

        /*
         * Job 3: return output top 3 descending (k, v)
         * k = cityInsurance#stateInsurance
         * v = times of appearance in list
         */

        if (success) {
            Job jobThird = new Job();

            jobThird.setJobName("Count and sort");
            jobThird.setJarByClass(ProcessLogs.class);

            // FileInputFormat.setInputPaths(jobSecond, new Path(args[1]) +
            // "/part-r-00000");
//            FileInputFormat.setInputPaths(jobThird, new Path(args[2] + "/part-r-00000"));
//            FileOutputFormat.setOutputPath(jobThird, new Path(args[3]));
            FileInputFormat.setInputPaths(jobThird, new Path(args[0]));
            FileOutputFormat.setOutputPath(jobThird, new Path(args[1]));

            // Sort descending key
//            jobThird.setSortComparatorClass(DescendingIntWritableComparable.class);

            jobThird.setMapperClass(MedicareMapperThird.class);
            jobThird.setReducerClass(MedicareReducerThird.class);

            jobThird.setMapOutputKeyClass(Text.class);
            jobThird.setMapOutputValueClass(IntWritable.class);

            jobThird.setOutputKeyClass(Text.class);
            jobThird.setOutputValueClass(IntWritable.class);

            success = jobThird.waitForCompletion(true);

            System.exit(success ? 0 : -1);

        } else {
            System.exit(-1);
        }
    }
}
