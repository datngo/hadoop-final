package finalproject.problem2a;

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

       
       Text dataReducerKey = createDataReducerKey();
       
       reduceDriver.withInput(dataReducerKey, values);
       
       String reducerOutputKey = "1234";
       Integer reducerOutputvalue = 2;

       reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new IntWritable(reducerOutputvalue));

       reduceDriver.runTest();
   }
   

   @Test
   public void testMapReduce() {
       Text testDataInput = createTestInput();
       Text testDataInput2 = createTestInput2();
       
       mapReduceDriver.withInput(new LongWritable(1), testDataInput);
       mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
       

       mapReduceDriver.addOutput(new Text("1234"), new IntWritable(Integer.parseInt("2")));
       mapReduceDriver.runTest();
   }

   private Text createDataReducerKey() {
       Text dataReducer = new Text("1234");
       return dataReducer;
   }
   
   private IntWritable createDataReducerValue(){
       return new IntWritable(1);
   }

   private Text createDataReducerKey2() {
       Text dataReducer = new Text("292");
       return dataReducer;
   }
   
   private Text createDataReducerValue2(){
       String codeInsurance = "10066";
       Double billRequest = 7646.363636;
       
       return new Text(codeInsurance.toString() + "#" + billRequest.toString());
   }

   private Text createTestInput() {
       return new Text("049#1234\t56757.5");
   }
   //finalPrjData
   private Text createTestInput2(){
       return new Text("292#1234\t7646.363636");
   }

}
