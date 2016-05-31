package finalproject.problem1;

import java.lang.reflect.Array;
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
    MapDriver<LongWritable, Text, MedicareWritable, DoubleWritable> mapDriver, mapSecondDriver;
    ReduceDriver<MedicareWritable, DoubleWritable, Text, DoubleWritable> reduceDriver, reduceSecondDriver;
    MapReduceDriver<LongWritable, Text, MedicareWritable, DoubleWritable, Text, DoubleWritable> mapReduceDriver, mapReduceSecondDriver;

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
        mapDriver = new MapDriver<LongWritable, Text, MedicareWritable, DoubleWritable>();
        mapDriver.setMapper(mapper); /**/

        /*
         * Set up the reducer test harness.
         */
        
        MedicareReducer reducer = new MedicareReducer();
        reduceDriver = new ReduceDriver<MedicareWritable, DoubleWritable, Text, DoubleWritable>();
        reduceDriver.setReducer(reducer);

        /*
         * Set up the mapper/reducer test harness.
         */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, MedicareWritable, DoubleWritable, Text, DoubleWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        
    }

    @Test
    public void testMapper() {
        Text tExt = createTestInput();
        mapDriver.withInput(new LongWritable(1), tExt);
        MedicareWritable dataReducer = createTestMediData();

        mapDriver.withOutput(dataReducer,
                new DoubleWritable(Double.parseDouble("56757.5")));

        mapDriver.runTest();
    }

    
    @Test
    public void testReducer() {

        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(Double.parseDouble("56757.5")));

        MedicareWritable dataReducer = createTestMediData();
        reduceDriver.withInput(dataReducer, values);

        reduceDriver.withOutput(new Text("00"),
                new DoubleWritable(Double.parseDouble("0")));

        reduceDriver.runTest();
    }
    

    @Test
    public void testMapReduce() {
        Text testDataInput = createTestInput();
        Text testDataInput2 = createTestInput2();
        
        mapReduceDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceDriver.withInput(new LongWritable(1), testDataInput2);
        

        mapReduceDriver.addOutput(new Text("292"), new DoubleWritable(Double.parseDouble("0.581481796")));
        mapReduceDriver.runTest();
    }

    private MedicareWritable createTestMediData() {
        Text codeTreatment = new Text();
        //DoubleWritable billRequest = new DoubleWritable();

        codeTreatment.set("00");
        //billRequest.set(56757.5);

        MedicareWritable dataReducer = new MedicareWritable();
        dataReducer.set(codeTreatment);
        return dataReducer;
    }
    

    private Text createTestInput() {
        return new Text(
                "292	50226	AHMC ANAHEIM REGIONAL MEDICAL CENTER	1111 W LA PALMA AVENUE	ANAHEIM	CA	92801	CA - Orange County	16	56757.5	7189.8125");
    }
    //finalPrjData
    private Text createTestInput2(){
        return new Text("292\t10066\tFLORALA MEMORIAL HOSPITAL\t24273 FIFTH AVENUE\tFLORALA\tAL\t36442\tFL - Pensacola\t11\t7646.363636\t5966.454545");
    }

}
