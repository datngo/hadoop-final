/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

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
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver, mapSecondDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver, reduceSecondDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver, mapReduceSecondDriver;

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
        mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
        mapDriver.setMapper(mapper); /**/

        /*
         * Set up the reducer test harness.
         */
        
        MedicareReducer reducer = new MedicareReducer();
        reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
        reduceDriver.setReducer(reducer);

        /*
         * Set up the mapper/reducer test harness.
         */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        
    }

    @Test
    public void testMapper() {
        Text tExt = createTestInput();
        mapDriver.withInput(new LongWritable(1), tExt);
        Text outputKey = createTestOutputKey();

        mapDriver.withOutput(outputKey,
                new DoubleWritable(Double.parseDouble("56757.5")));

        mapDriver.runTest();
    }

    
    @Test
    public void testReducer() {

        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(Double.parseDouble("56757.5")));
        values.add(new DoubleWritable(Double.parseDouble("7646.363636")));
        values.add(new DoubleWritable(Double.parseDouble("12321.1")));

        Text outputKey = createTestOutputKey();
        reduceDriver.withInput(outputKey, values);

        reduceDriver.withOutput(new Text("292"),
                new DoubleWritable(Double.parseDouble("0.26856889456801675")));

        reduceDriver.runTest();
    }
    

    @Test
    public void testMapReduce() {
        Text testDataInput = createTestInput();
        Text testDataInput2 = createTestInput2();
        
        mapReduceDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
        

        mapReduceDriver.addOutput(new Text("292"), new DoubleWritable(Double.parseDouble("0.5814817956059218")));
        mapReduceDriver.runTest();
    }
    
    /**
     * Create test data.
     * */

    private Text createTestOutputKey() {
    	return new Text("292");
    }

    private Text createTestInput() {
        return new Text(
                "292	50226	AHMC ANAHEIM REGIONAL MEDICAL CENTER	1111 W LA PALMA AVENUE	ANAHEIM	CA	92801	CA - Orange County	16	56757.5	7189.8125");
    }
    
    private Text createTestInput2(){
        return new Text("292\t10066\tFLORALA MEMORIAL HOSPITAL\t24273 FIFTH AVENUE\tFLORALA\tAL\t36442\tFL - Pensacola\t11\t7646.363636\t5966.454545");
    }

}
