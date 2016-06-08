/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem4;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * This is the main process for testing Job 3 of this program.
 * </pre>
 * 
 * </p>
 * 
 */
public class TestProcessMedicareThird {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and a mapper and
     * a reducer working together.
     */
    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    ReduceDriver<IntWritable, Text, Text, NullWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, NullWritable> mapReduceDriver;

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
        reduceDriver = new ReduceDriver<IntWritable, Text, Text, NullWritable>();
        reduceDriver.setReducer(reducer);

        /*
         * Set up the mapper/reducer test harness.
         */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, NullWritable>();
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
//        Integer reducerOutputvalue = 10;

        reduceDriver.withOutput(new Text(reducerOutputKey.toString()), NullWritable.get());

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        Text testDataInput = createTestInput();
        Text testDataInput2 = createTestInput2();

        mapReduceDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceDriver.withInput(new LongWritable(1), testDataInput2);

        mapReduceDriver.addOutput(new Text("1234"), NullWritable.get());
        mapReduceDriver.addOutput(new Text("9876"), NullWritable.get());
        mapReduceDriver.runTest();
    }

    /**
     * Create test data
     * */
    private IntWritable createDataReducerKey() {
        return new IntWritable(10);
    }

    private Text createDataReducerValue() {
        return new Text("1234");
    }

    private Text createTestInput() {
        return new Text("1234\t10");
    }

    // finalPrjData
    private Text createTestInput2() {
        return new Text("9876\t13");
    }

}
