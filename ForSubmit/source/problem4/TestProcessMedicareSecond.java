/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * <p>
 * 
 * <pre>
 * This is the main process for testing Job 2 of this program.
 * </pre>
 * 
 * </p>
 * 
 */
public class TestProcessMedicareSecond {

    /*
    * Declare harnesses that let you test a mapper, a reducer, and a mapper and
    * a reducer working together.
    */
   MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
   ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
   MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

   /*
    * Set up the test. This method will be called before every test.
    */
   @Before
   public void setUp() {

       /*
        * Set up the mapper test harness.
        */
       MedicareMapperSecond mapper = new MedicareMapperSecond();
       // LongWritable, Text, WebLogWritable, IntWritable
       mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
       mapDriver.setMapper(mapper); /**/

       /*
        * Set up the reducer test harness.
        */
       
       MedicareReducerSecond reducer = new MedicareReducerSecond();
       reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
       reduceDriver.setReducer(reducer);

       /*
        * Set up the mapper/reducer test harness.
        */
       mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
       mapReduceDriver.setMapper(mapper);
       mapReduceDriver.setReducer(reducer);
       
   }

   @Test
   public void testMapper() {
       Text tExt = createTestInput();

       mapDriver.withInput(new LongWritable(1), tExt);
       
       Text dataReducerKey = createDataReducerKey();
       IntWritable dataReducerValue = createDataReducerValue();

       mapDriver.withOutput(dataReducerKey, dataReducerValue);

       mapDriver.runTest();
   }

   
   @Test
   public void testReducer() {

       List<IntWritable> values = new ArrayList<IntWritable>();
       values.add(new IntWritable(1));
       values.add(new IntWritable(1));
       values.add(new IntWritable(1));
       values.add(new IntWritable(1));
       values.add(new IntWritable(1));
       
       Text dataReducerKey = createDataReducerKey();
       
       reduceDriver.withInput(dataReducerKey, values);
       
       String reducerOutputKey = "292";
       Integer reducerOutputvalue = 5;

       reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new IntWritable(reducerOutputvalue));

       reduceDriver.runTest();
   }
   

   @Test
   public void testMapReduce() {
       Text testDataInput = createTestInput();
       Text testDataInput2 = createTestInput2();
       Text testDataInput3 = createTestInput3();
       Text testDataInput4 = createTestInput4();
       
       mapReduceDriver.withInput(new LongWritable(1), testDataInput);
       mapReduceDriver.withInput(new LongWritable(2), testDataInput2);
       mapReduceDriver.withInput(new LongWritable(3), testDataInput3);
       mapReduceDriver.withInput(new LongWritable(4), testDataInput4);
       
       mapReduceDriver.addOutput(new Text("001"), new IntWritable(1));
       mapReduceDriver.addOutput(new Text("002"), new IntWritable(1));
       mapReduceDriver.addOutput(new Text("292"), new IntWritable(2));
       mapReduceDriver.runTest();
   }

   /**
    * Create test data
    * */
   private Text createDataReducerKey() {
       Text dataReducer = new Text("292");
       return dataReducer;
   }
   
   private IntWritable createDataReducerValue(){
       return new IntWritable(1);
   }


   private Text createTestInput() {
       return new Text("292\t56757.5");
   }

   private Text createTestInput2(){
       return new Text("292\t7646.363636");
   }

   private Text createTestInput3() {
       return new Text("001\t1234.98");
   }

   private Text createTestInput4(){
       return new Text("002\t7646.363636");
   }
}
