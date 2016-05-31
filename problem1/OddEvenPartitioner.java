package finalproject.problem1;

import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * CRITICAL NOTE:
 * we must change extends Partitioner<Text, Text>
 * into extends Partitioner<WebLogWritable, IntWritable>
 * */

public class OddEvenPartitioner<K2, V2> extends Partitioner<MedicareWritable, IntWritable> implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> allPartitions = new HashMap<String, Integer>();

  /**
   * Set up the months hash map in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    allPartitions.put("Odd", 0);
    allPartitions.put("Even", 1);
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  /**
   * CRITICAL NOTE:
   * we must change getPartition(Text key, Text value, int numReduceTasks) 
   * into getPartition(WebLogWritable key, IntWritable value, int numReduceTasks) 
   * */
  /**
   * CRITICAL NOTE 1:
   * value of numReduceTasks will be set by job.setNumReduceTasks(2); at class OddEventParititioner.java
   * 
   */
  public int getPartition(MedicareWritable key, IntWritable value, int numReduceTasks) {
	  String ipAsAKey = key.toString();
	  System.out.println("ipAsAKey == "+ipAsAKey);
	  String ipParts[] = ipAsAKey.split("\\.");
	  System.out.println("ipParts.length == "+ipParts.length);
	  System.out.println("ipParts[0] == "+ipParts[0]);
	  
	  int lastPart = Integer.parseInt(ipParts[3]);
	  int afterMode = lastPart%2;
	  String partitionKeyToMatch = "Odd";
	  if (afterMode == 0) {
		  partitionKeyToMatch = "Even";
	  }
    return (int) (allPartitions.get(partitionKeyToMatch));
  }
}
