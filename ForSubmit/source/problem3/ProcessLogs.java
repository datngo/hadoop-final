/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

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

        if (args.length != 5) {
            System.out
                    .printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2 - input 3 dir><output 3 - input 4 dir><output 4>\n");
            System.exit(-1);
        }

        /*
         * Job 1: Return list (k, v) grouping by k
         * k = codeTreatment#cityInsurance#stateInsurance
         * v = average of all billRequest 
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
        job.setMapOutputValueClass(DoubleWritable.class);

        /**
         * Set type of output Key for Reducer.
         * Set type of output Value for Reducer.
         * They are also the data output of Job 1.
         * */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        boolean success = job.waitForCompletion(true);
//        boolean success = true;

        /*
         * Job 2: return (k, v)
         * k = cityInsurance#stateInsurance
         * v = return 1 corresponding with maximum of average of all billRequest
         */
        if (success) {
            Job jobSecond = new Job();

            jobSecond.setJobName("List of geo-locations have the highest average billRequest");
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
            jobSecond.setMapOutputValueClass(Text.class);

            /**
             * Set type of output Key for Reducer.
             * Set type of output Value for Reducer.
             * They are also the data input for Job 3.
             * */
            jobSecond.setOutputKeyClass(Text.class);
            jobSecond.setOutputValueClass(DoubleWritable.class);

            success = jobSecond.waitForCompletion(true);

        } else {
            System.exit(-1);
        }

        /*
         * Job 3: return output (k, v)
         * k = cityInsurance#stateInsurance
         * v = times of appearance in list
         */

        if (success) {
            Job jobThird = new Job();

            jobThird.setJobName("Counting times");
            jobThird.setJarByClass(ProcessLogs.class);

            /**
             * Set input directory for Mapper. It is also the output data from Job 2
             * Set output directory for Reducer.
             * */
            FileInputFormat.setInputPaths(jobThird, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobThird, new Path(args[3]));

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
            jobThird.setMapOutputKeyClass(Text.class);
            jobThird.setMapOutputValueClass(IntWritable.class);

            /**
             * Set type of output Key for Reducer.
             * Set type of output Value for Reducer.
             * They are also the data input for Job 4.
             * */
            jobThird.setOutputKeyClass(Text.class);
            jobThird.setOutputValueClass(IntWritable.class);

            success = jobThird.waitForCompletion(true);

        } else {
            System.exit(-1);
        }
        

        /*
         * Job 4: return output top 3 descending (k, v)
         * k = cityInsurance#stateInsurance
         * v = times of appearance in list
         */

        if (success) {
            Job jobFourth = new Job();

            jobFourth.setJobName("Sort and top");
            jobFourth.setJarByClass(ProcessLogs.class);

            /**
             * Set input directory for Mapper. It is also the output data from Job 3.
             * Set output directory for Reducer.
             * */
            FileInputFormat.setInputPaths(jobFourth, new Path(args[3] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(jobFourth, new Path(args[4]));

            /**
             * Set custom comparator to sort descending key in Sort phase.
             * */
            jobFourth.setSortComparatorClass(DescendingIntWritableComparable.class);

            /**
             * Set mapper class for Mapper.
             * Set reducer class for Reducer.
             * */
            jobFourth.setMapperClass(MedicareMapperFourth.class);
            jobFourth.setReducerClass(MedicareReducerFourth.class);

            /**
             * Set type of output Key for Mapper. 
             * Set type of output Value for Mapper.
             * They are also the data input for Reducer.
             * */
            jobFourth.setMapOutputKeyClass(IntWritable.class);
            jobFourth.setMapOutputValueClass(Text.class);

            /**
             * Set type of output Key for Reducer.
             * Set type of output Value for Reducer.
             * They are also the data output for this program.
             * */
            jobFourth.setOutputKeyClass(Text.class);
            jobFourth.setOutputValueClass(IntWritable.class);

            success = jobFourth.waitForCompletion(true);

            System.exit(success ? 0 : -1);

        } else {
            System.exit(-1);
        }
    }
}
