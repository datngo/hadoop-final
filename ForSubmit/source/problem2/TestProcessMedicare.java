/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem2;

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
 * 
 * <p>
 * 
 * <pre>
 * This is the main process for testing Job 1 of this program.
 * </pre>
 * 
 * </p>
 * 
 */

public class TestProcessMedicare {

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
        MedicareMapper mapper = new MedicareMapper();
        // LongWritable, Text, WebLogWritable, IntWritable
        mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        mapDriver.setMapper(mapper); /**/

        /*
         * Set up the reducer test harness.
         */
        
        MedicareReducer reducer = new MedicareReducer();
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
        values.add(new Text("50226#56757.5"));
        values.add(new Text("10066#7646.363636"));

        
        Text dataReducerKey = createDataReducerKey();
        
        reduceDriver.withInput(dataReducerKey, values);
        
        String reducerOutputKey = "292#50226";
        Double reducerOutputvalue = 56757.5;

        reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new DoubleWritable(reducerOutputvalue));

        reduceDriver.runTest();
    }
    

    @Test
    public void testMapReduce() {
        Text testDataInput = createTestInput();
        Text testDataInput2 = createTestInput2();
        
        mapReduceDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
        

        mapReduceDriver.addOutput(new Text("292#50226"), new DoubleWritable(Double.parseDouble("56757.5")));
        mapReduceDriver.runTest();
    }

    /**
     * Create data test
     * */
    private Text createDataReducerKey() {
        Text dataReducer = new Text("292");
        return dataReducer;
    }
    
    private Text createDataReducerValue(){
        String codeInsurance = "50226";
        Double billRequest = 56757.5;
        
        return new Text(codeInsurance.toString() + "#" + billRequest.toString());
    }
    private Text createTestInput() {
        return new Text("292\t50226\tAHMC ANAHEIM REGIONAL MEDICAL CENTER\t1111 W LA PALMA AVENUE\tANAHEIM\tCA\t92801\tCA - Orange County\t16\t56757.5\t7189.8125");
    }
    //finalPrjData
    private Text createTestInput2(){
        return new Text("292\t10066\tFLORALA MEMORIAL HOSPITAL\t24273 FIFTH AVENUE\tFLORALA\tAL\t36442\tFL - Pensacola\t11\t7646.363636\t5966.454545");
    }

}
