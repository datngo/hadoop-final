/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * <p>
 * 
 * <pre>
 * This is the main Driver of Map-Reduce project. Control all flows of this program.
 * </pre>
 * 
 * </p>
 * 
 */

public class ProcessLogs {

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out
                    .printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2 - input 3 dir><output 3>\n");
            System.exit(-1);
        }

        /*
         * Job 1: Return list (k, v) grouping by k
         * k = codeTreatment
         * v = codeInsurance + max of all (billRequest-billActual) 
         */
        Job job = new Job();
        job.setJarByClass(ProcessLogs.class);
        job.setJobName("Process Logs 1");

        /**
         * Set input directory for Mapper.
         * Set output directory for Reducer.
         * */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /**
         * Set mapper class for Mapper.
         * Set reducer class for Reducer.
         * */
        job.setMapperClass(MedicareMapper.class);
        job.setReducerClass(MedicareReducer.class);

        /**
         * Set type of output Key for Mapper. 
         * Set type of output Value for Mapper.
         * They are also the data input for Reducer.
         * */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /**
         * Set type of output Key for Reducer.
         * Set type of output Value for Reducer.
         * They are also the data output of Job 1.
         * */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /**
         * Initialize boolean variable to keep track of each job's status
         * */
        boolean success = job.waitForCompletion(true);

        /*
         * Job 2: return (k, v)
         * k = codeInsurance
         * v = times of appearance in list
         */
        if (success) {
            Job jobSecond = new Job();

            jobSecond.setJobName("Count number of appearance times");
            jobSecond.setJarByClass(ProcessLogs.class);

            /**
             * Set input directory for Mapper. It is also the output data from Job 1.
             * Set output directory for Reducer.
             * */
            FileInputFormat.setInputPaths(jobSecond, new Path(args[1] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));

            /**
             * Set mapper class for Mapper.
             * Set reducer class for Reducer.
             * */
            jobSecond.setMapperClass(MedicareMapperSecond.class);
            jobSecond.setReducerClass(MedicareReducerSecond.class);

            /**
             * Set type of output Key for Mapper. 
             * Set type of output Value for Mapper.
             * They are also the data input for Reducer.
             * */
            jobSecond.setMapOutputKeyClass(Text.class);
            jobSecond.setMapOutputValueClass(IntWritable.class);

            /**
             * Set type of output Key for Reducer.
             * Set type of output Value for Reducer.
             * They are also the data output for Job 2.
             * */
            jobSecond.setOutputKeyClass(Text.class);
            jobSecond.setOutputValueClass(IntWritable.class);

            success = jobSecond.waitForCompletion(true);

        } else {
            System.exit(-1);
        }

        /*
         * Job 3: return output (k, v)
         * k = codeTreatment
         * v = times of appearance in list
         */

        if (success) {
            Job jobThird = new Job();

            jobThird.setJobName("Counting times");
            jobThird.setJarByClass(ProcessLogs.class);

            /**
             * Set input directory for Mapper. It is also the output data from Job 2.
             * Set output directory for Reducer.
             * */
            FileInputFormat.setInputPaths(jobThird, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobThird, new Path(args[3]));

            /**
             * Set custom comparator to sort descending key in Sort phase.
             * */
            jobThird.setSortComparatorClass(DescendingIntWritableComparable.class);

            /**
             * Set mapper class for Mapper.
             * Set reducer class for Reducer.
             * */
            jobThird.setMapperClass(MedicareMapperThird.class);
            jobThird.setReducerClass(MedicareReducerThird.class);

            /**
             * Set type of output Key for Mapper. 
             * Set type of output Value for Mapper.
             * They are also the data input for Reducer.
             * */
            jobThird.setMapOutputKeyClass(IntWritable.class);
            jobThird.setMapOutputValueClass(Text.class);

            /**
             * Set type of output Key for Reducer.
             * Set type of output Value for Reducer.
             * They are also the data output of this program.
             * */
            jobThird.setOutputKeyClass(Text.class);
            jobThird.setOutputValueClass(IntWritable.class);

            success = jobThird.waitForCompletion(true);

            System.exit(success ? 0 : -1);

        } else {
            System.exit(-1);
        }
    }
}
