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

public class TestProcessMedicareThird {

    /*
    * Declare harnesses that let you test a mapper, a reducer, and a mapper and
    * a reducer working together.
    */
   MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
   ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;
   MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> mapReduceDriver;

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
       mapDriver = new MapDriver<LongWritable, Text, IntWritable, Text>();
       mapDriver.setMapper(mapper); /**/

       /*
        * Set up the reducer test harness.
        */
       
       MedicareReducerThird reducer = new MedicareReducerThird();
       reduceDriver = new ReduceDriver<IntWritable, Text, Text, IntWritable>();
       reduceDriver.setReducer(reducer);

       /*
        * Set up the mapper/reducer test harness.
        */
       mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable>();
       mapReduceDriver.setMapper(mapper);
       mapReduceDriver.setReducer(reducer);
       
   }

   @Test
   public void testMapper() {
       Text tExt = createTestInput();

       mapDriver.withInput(new LongWritable(1), tExt);
       
       IntWritable dataReducerKey = createDataReducerKey();
       Text dataReducerValue = createDataReducerValue();

       mapDriver.withOutput(dataReducerKey, dataReducerValue);

       mapDriver.runTest();
   }

   
   @Test
   public void testReducer() {

       List<Text> values = new ArrayList<Text>();
       values.add(new Text("1234"));

       
       IntWritable dataReducerKey = createDataReducerKey();
       
       reduceDriver.withInput(dataReducerKey, values);
       
       String reducerOutputKey = "1234";
       Integer reducerOutputvalue = 8;

       reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new IntWritable(reducerOutputvalue));

       reduceDriver.runTest();
   }
   

   @Test
   public void testMapReduce() {
       Text testDataInput = createTestInput();
       Text testDataInput2 = createTestInput2();
       
       mapReduceDriver.withInput(new LongWritable(1), testDataInput);
       mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
       

       mapReduceDriver.addOutput(new Text("1234"), new IntWritable(Integer.parseInt("8")));
       mapReduceDriver.addOutput(new Text("9876"), new IntWritable(Integer.parseInt("13")));
       mapReduceDriver.runTest();
   }

   private IntWritable createDataReducerKey() {
       return new IntWritable(8);
   }
   
   private Text createDataReducerValue(){
       return new Text("1234");
   }

   private IntWritable createDataReducerKey2() {
       return new IntWritable(13);
   }
   
   private Text createDataReducerValue2(){
       return new Text("9876");
   }

   private Text createTestInput() {
       return new Text("1234\t8");
   }
   //finalPrjData
   private Text createTestInput2(){
       return new Text("9876\t13");
   }

}
