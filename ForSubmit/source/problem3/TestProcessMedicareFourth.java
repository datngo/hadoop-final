/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem3;

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
* <p>
* 
* <pre>
 * This is the main process for testing Job 4 of this program.
* </pre>
* 
* </p>
* 
*/
public class TestProcessMedicareFourth {

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
       MedicareMapperThird mapper = new MedicareMapperThird();
       // LongWritable, Text, WebLogWritable, IntWritable
       mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
       mapDriver.setMapper(mapper); /**/

       /*
        * Set up the reducer test harness.
        */
       
       MedicareReducerThird reducer = new MedicareReducerThird();
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
       
       Text dataReducerKey = createDataReducerKey();
       
       reduceDriver.withInput(dataReducerKey, values);
       
       String reducerOutputKey = "1234";
       Integer reducerOutputvalue = 3;

       reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new IntWritable(reducerOutputvalue));

       reduceDriver.runTest();
   }
   

   @Test
   public void testMapReduce() {
       Text testDataInput = createTestInput();
       Text testDataInput2 = createTestInput2();
       
       mapReduceDriver.withInput(new LongWritable(1), testDataInput);
       mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
       

       mapReduceDriver.addOutput(new Text("1234"), new IntWritable(Integer.parseInt("1")));
       mapReduceDriver.addOutput(new Text("9876"), new IntWritable(Integer.parseInt("1")));
       mapReduceDriver.runTest();
   }


   /**
    * Create test data
    * */
   private Text createDataReducerKey() {
       return new Text("1234");
   }
   
   private IntWritable createDataReducerValue(){
       return new IntWritable(1);
   }

   private Text createTestInput() {
       return new Text("1234\t8");
   }

   private Text createTestInput2(){
       return new Text("9876\t13");
   }

}
