/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

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

    if (args.length != 3) {
      System.out.printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2>\n");
      System.exit(-1);
    }

    /**
     * Job 1: calculate the relative variance of each medicare code
     */
    
    Job job = new Job();
    job.setJarByClass(ProcessLogs.class);
    job.setJobName("Calculate relative variance");
    
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

    /**
     * Initialize boolean variable to keep track of each job's status
     * */
    boolean success = job.waitForCompletion(true);
    
    /**
     * Job 2: Sort descending relative variance and return top 3 of them. 
     */
    if(success){
        Job jobSecond = new Job();
        
        jobSecond.setJobName("Sort relative variance");
        jobSecond.setJarByClass(ProcessLogs.class);
        
        /**
         * Set input directory for Mapper. It is also the output data from Job 1.
         * Set output directory for Reducer.
         * */
        FileInputFormat.setInputPaths(jobSecond, new Path(args[1]) + "/part-r-00000");
        FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));
        
        /**
         * Set custom comparator to sort descending key in Sort phase.
         * */
        jobSecond.setSortComparatorClass(DescendingDoubleWritableComparable.class);

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
        jobSecond.setMapOutputKeyClass(DoubleWritable.class);
        jobSecond.setMapOutputValueClass(Text.class);

        /**
         * Set type of output Key for Reducer.
         * Set type of output Value for Reducer.
         * They are also the data output of this program.
         * */
        jobSecond.setOutputKeyClass(Text.class);
        jobSecond.setOutputValueClass(DoubleWritable.class);
        
        success = jobSecond.waitForCompletion(true);
        
        System.exit(success ? 0 : -1);
        
    }else{
        System.exit(-1);
    }
  }
}
