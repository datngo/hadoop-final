/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/** 
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
   MapDriver<LongWritable, Text, Text, Text> mapDriver;
   ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
   MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;

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
       mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
       mapDriver.setMapper(mapper); /**/

       /*
        * Set up the reducer test harness.
        */
       
       MedicareReducerSecond reducer = new MedicareReducerSecond();
       reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
       reduceDriver.setReducer(reducer);

       /*
        * Set up the mapper/reducer test harness.
        */
       mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
       mapReduceDriver.setMapper(mapper);
       mapReduceDriver.setReducer(reducer);
       
   }

   @Test
   public void testMapper() {
       Text tExt = createTestInput();

       mapDriver.withInput(new LongWritable(1), tExt);
       
       Text dataReducerKey = createDataReducerKey();
       Text dataReducerValue = createDataReducerValue();

       mapDriver.withOutput(dataReducerKey, dataReducerValue);

       mapDriver.runTest();
   }

   
   @Test
   public void testReducer() {

       List<Text> values = new ArrayList<Text>();
       values.add(new Text("ANAHEIM#CA#56757.5"));
       values.add(new Text("ANAHEIM#CA#7646.363636"));

       
       Text dataReducerKey = createDataReducerKey();
       
       reduceDriver.withInput(dataReducerKey, values);
       
       String reducerOutputKey = "ANAHEIM#CA";
       double reducerOutputvalue = 56757.5;

       reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new DoubleWritable(reducerOutputvalue));

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
       
       mapReduceDriver.addOutput(new Text("ANAHEIM#CA"), new DoubleWritable(1234.98));
       mapReduceDriver.addOutput(new Text("ANAHEIM#CA"), new DoubleWritable(7646.363636));
       mapReduceDriver.addOutput(new Text("ANAHEIM#CA"), new DoubleWritable(56757.5));
       mapReduceDriver.runTest();
   }


   /**
    * Create test data
    * */
   private Text createDataReducerKey() {
       Text dataReducer = new Text("292");
       return dataReducer;
   }
   
   private Text createDataReducerValue(){
       return new Text("ANAHEIM#CA#56757.5");
   }


   private Text createTestInput() {
       return new Text("292#ANAHEIM#CA\t56757.5");
   }

   private Text createTestInput2(){
       return new Text("292#ANAHEIM#CA\t7646.363636");
   }

   private Text createTestInput3() {
       return new Text("001#ANAHEIM#CA\t1234.98");
   }

   private Text createTestInput4(){
       return new Text("002#ANAHEIM#CA\t7646.363636");
   }
}
