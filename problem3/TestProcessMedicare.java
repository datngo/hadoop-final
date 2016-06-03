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

public class TestProcessMedicare {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and a mapper and
     * a reducer working together.
     */
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

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
        
        Text dataReducerKey = createDataReducerKey();
        DoubleWritable dataReducerValue = createDataReducerValue();

        mapDriver.withOutput(dataReducerKey, dataReducerValue);

        mapDriver.runTest();
    }

    
    @Test
    public void testReducer() {

        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(56757.5));
        values.add(new DoubleWritable(7646.363636));

        
        Text dataReducerKey = createDataReducerKey();
        
        reduceDriver.withInput(dataReducerKey, values);
        
        String reducerOutputKey = "292#ANAHEIM#CA";
        Double reducerOutputvalue = 32201.931818;

        reduceDriver.withOutput(new Text(reducerOutputKey.toString()), new DoubleWritable(reducerOutputvalue));

        reduceDriver.runTest();
    }
    

    @Test
    public void testMapReduce() {
        Text testDataInput = createTestInput();
        Text testDataInput2 = createTestInput2();
        
        mapReduceDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
        

        mapReduceDriver.addOutput(new Text("292#ANAHEIM#CA"), new DoubleWritable(32201.931818));
        mapReduceDriver.runTest();
    }

    private Text createDataReducerKey() {
        Text dataReducer = new Text("292#ANAHEIM#CA");
        return dataReducer;
    }
    
    private DoubleWritable createDataReducerValue(){
        return new DoubleWritable(56757.5);
    }

    private Text createDataReducerKey2() {
        Text dataReducer = new Text("292#ANAHEIM#CA");
        return dataReducer;
    }
    
    private DoubleWritable createDataReducerValue2(){
        return new DoubleWritable(7646.363636);
    }

    private Text createTestInput() {
        return new Text("292\t50226\tAHMC ANAHEIM REGIONAL MEDICAL CENTER\t1111 W LA PALMA AVENUE\tANAHEIM\tCA\t92801\tCA - Orange County\t16\t56757.5\t7189.8125");
    }
    //finalPrjData
    private Text createTestInput2(){
        return new Text("292\t10066\tFLORALA MEMORIAL HOSPITAL\t24273 FIFTH AVENUE\tANAHEIM\tCA\t36442\tFL - Pensacola\t11\t7646.363636\t5966.454545");
    }

}
