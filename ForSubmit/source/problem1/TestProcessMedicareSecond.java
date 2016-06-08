/*
 * Author: Group 4
 * Created Date: 2016/06/06
 */
package finalproject.problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
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
    MapDriver<LongWritable, Text, DoubleWritable, Text> mapSecondDriver;
    ReduceDriver<DoubleWritable, Text, Text, NullWritable> reduceSecondDriver;
    MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable> mapReduceSecondDriver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        /*
         * Set up the mapper secondary test
         */
        MedicareMapperSecond mapperSecond = new MedicareMapperSecond();
        mapSecondDriver = new MapDriver<LongWritable, Text, DoubleWritable, Text>();
        mapSecondDriver.setMapper(mapperSecond);

        /*
         * Set up the reducer secondary test
         */
        MedicareReducerSecond reduceSecond = new MedicareReducerSecond();
        reduceSecondDriver = new ReduceDriver<DoubleWritable, Text, Text, NullWritable>();
        reduceSecondDriver.setReducer(reduceSecond);

        /*
         * Set up the mapper/reducer secondary test
         */
        mapReduceSecondDriver = new MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable>();
        mapReduceSecondDriver.setMapper(mapperSecond);
        mapReduceSecondDriver.setReducer(reduceSecond);
    }

    @Test
    public void testMapperSecond() {
        Text tExt = createTestInputData();
        mapSecondDriver.withInput(new LongWritable(1), tExt);

        DoubleWritable dataReducer = createTestMapperOutputKey();

        mapSecondDriver.withOutput(dataReducer, new Text("292"));

        mapSecondDriver.runTest();
    }

    @Test
    public void testReducerSecond() {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("292"));

        DoubleWritable dataReducer = createTestMapperOutputKey();
        reduceSecondDriver.withInput(dataReducer, values);

        reduceSecondDriver.withOutput(new Text("292"), NullWritable.get());

        reduceSecondDriver.runTest();
    }

    @Test
    public void testMapReduceSecond() throws IOException {
        Text testDataInput = createTestInputData();
        Text testDataInput2 = createTestInputData2();
        Text testDataInput3 = createTestInputData3();
        Text testDataInput4 = createTestInputData4();

        mapReduceSecondDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceSecondDriver.withInput(new LongWritable(2), testDataInput2);
        mapReduceSecondDriver.withInput(new LongWritable(3), testDataInput3);
        mapReduceSecondDriver.withInput(new LongWritable(4), testDataInput4);

        mapReduceSecondDriver.addOutput(new Text("292"), NullWritable.get());
        mapReduceSecondDriver.addOutput(new Text("188"), NullWritable.get());
        mapReduceSecondDriver.addOutput(new Text("04"), NullWritable.get());

        mapReduceSecondDriver.runTest();
    }

    /**
     * Create test data.
     * */
    private DoubleWritable createTestMapperOutputKey() {
        return new DoubleWritable(Double.parseDouble("0.18080510831413377"));
    }

    private Text createTestInputData() {
        return new Text("292\t0.18080510831413377");
    }

    private Text createTestInputData2() {
        return new Text("188\t0.29977");
    }

    private Text createTestInputData3() {
        return new Text("222\t10.6");
    }

    private Text createTestInputData4() {
        return new Text("04\t7.2");
    }

}
