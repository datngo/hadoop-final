package finalproject.problem1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ProcessLogs {

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.printf("Usage: ProcessLogs <input dir> <output 1 - input 2 dir><output 2>\n");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProcessLogs.class);
    job.setJobName("Process Logs");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MedicareMapper.class);
    job.setReducerClass(MedicareReducer.class);
    
//    CRITICAL NOTE: set output for mapper - we must refer to declaration of file WEBLOGMAPPER.JAVA
//     to findout correct parameters for 2 below statement
    job.setMapOutputKeyClass(MedicareWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class); // -- may be this line cause java.io.IOException: Type mismatch in key from map
    job.setOutputValueClass(DoubleWritable.class);

    boolean success = job.waitForCompletion(true);
    
    /* 
     * Job 2 
     */
    if(success){
        Job jobSecond = new Job();
        
        jobSecond.setJobName("Sort relative variance");
        jobSecond.setJarByClass(ProcessLogs.class);
        
        FileInputFormat.setInputPaths(jobSecond, new Path(args[1]) + "/part-r-00000");
        FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));
        
        //jobSecond.setSortComparatorClass(DescendingNumberComparator.class);

        jobSecond.setMapperClass(MedicareMapperSecond.class);
        jobSecond.setReducerClass(MedicareReducerSecond.class);
        
        //CRITICAL NOTE: set output for mapper - we must refer to declaration of file WEBLOGMAPPER.JAVA
        // to find-out correct parameters for 2 below statement
        jobSecond.setMapOutputKeyClass(MedicareWritableSecond.class);
        jobSecond.setMapOutputValueClass(DoubleWritable.class);

        jobSecond.setOutputKeyClass(Text.class); // -- may be this line cause java.io.IOException: Type mismatch in key from map
        jobSecond.setOutputValueClass(DoubleWritable.class);
        
        success = jobSecond.waitForCompletion(true);
        
        System.exit(success ? 0 : -1);
        
    }else{
        System.exit(-1);
    }
  }
}
