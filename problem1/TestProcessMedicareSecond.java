/**
 * 
 */
package finalproject.problem1;

import static org.junit.Assert.*;

import java.io.IOException;
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
 * @author training
 *
 */
public class TestProcessMedicareSecond {
    
    /*
     * Declare harnesses that let you test a mapper, a reducer, and a mapper and
     * a reducer working together.
     */
    MapDriver<LongWritable, Text, MedicareWritableSecond, DoubleWritable>  mapSecondDriver;
    ReduceDriver<MedicareWritableSecond, DoubleWritable, Text, DoubleWritable>  reduceSecondDriver;
    MapReduceDriver<LongWritable, Text, MedicareWritableSecond, DoubleWritable, Text, DoubleWritable> mapReduceSecondDriver;


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        /*
         * Set up the mapper secondary test 
         */
        MedicareMapperSecond mapperSecond = new MedicareMapperSecond();
        mapSecondDriver = new MapDriver<LongWritable, Text, MedicareWritableSecond, DoubleWritable>();
        mapSecondDriver.setMapper(mapperSecond);
        
        /*
         * Set up the reducer secondary test 
         */
        MedicareReducerSecond reduceSecond = new MedicareReducerSecond();
        reduceSecondDriver = new ReduceDriver<MedicareWritableSecond, DoubleWritable, Text, DoubleWritable>();
        reduceSecondDriver.setReducer(reduceSecond);
        
        /*
         * Set up the mapper/reducer secondary test 
         */
        mapReduceSecondDriver = new MapReduceDriver<LongWritable, Text, MedicareWritableSecond, DoubleWritable, Text, DoubleWritable>();
        mapReduceSecondDriver.setMapper(mapperSecond);
        mapReduceSecondDriver.setReducer(reduceSecond);
    }

//    @Test
//    public void test() {
//        fail("Not yet implemented");
//    }
    
//    @Test
//    public void testMapperSecond() {
//        Text tExt = createTestInputSecond();
//        mapSecondDriver.withInput(new LongWritable(1), tExt);
//        
//        MedicareWritable dataReducer = createTestDataSecond();
//
//        mapSecondDriver.withOutput(dataReducer,
//                new DoubleWritable(Double.parseDouble("0.18080510831413377")));
//
//        mapSecondDriver.runTest();
//    }
    
//    @Test
//    public void testReducerSecond(){
//        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
//        values.add(new DoubleWritable(Double.parseDouble("0.18080510831413377")));
//
//        MedicareWritableSecond dataReducer = createTestDataSecond();
//        reduceSecondDriver.withInput(dataReducer, values);
//
//        reduceSecondDriver.withOutput(new Text("292"),
//                new DoubleWritable(Double.parseDouble("0.18080510831413377")));
//
//        reduceSecondDriver.runTest();
//    }
    
    @Test
    public void testMapReduceSecond() throws IOException {
        Text testDataInput = createTestInputSecond();
        Text testDataInput2 = createTestInputSecond2();
        Text testDataInput3 = createTestInputSecond3();
        Text testDataInput4 = createTestInputSecond4();
        
        mapReduceSecondDriver.withInput(new LongWritable(1), testDataInput);
        mapReduceSecondDriver.withInput(new LongWritable(2), testDataInput2);
        mapReduceSecondDriver.withInput(new LongWritable(3), testDataInput3);
        mapReduceSecondDriver.withInput(new LongWritable(4), testDataInput4);

        mapReduceSecondDriver.addOutput(new Text("222"), new DoubleWritable(Double.parseDouble("10.6")));
        mapReduceSecondDriver.addOutput(new Text("04"), new DoubleWritable(Double.parseDouble("7.2")));
        mapReduceSecondDriver.addOutput(new Text("188"), new DoubleWritable(Double.parseDouble("0.29977")));
        mapReduceSecondDriver.addOutput(new Text("292"), new DoubleWritable(Double.parseDouble("0.18080510831413377")));
        
//        MedicareWritableSecond dataReducer = new MedicareWritableSecond();
//        dataReducer.set(new Text("222"), new DoubleWritable(Double.parseDouble("1.18080510831413377")));
//        
//        MedicareWritableSecond dataReducer2 = new MedicareWritableSecond();
//        dataReducer2.set(new Text("111"), new DoubleWritable(Double.parseDouble("2.18080510831413377")));
        
        //int result = myComparator.compare(dataReducer, dataReducer2);
        
        //System.out.println("result = " + result);
        
        mapReduceSecondDriver.runTest();
    }
    
    private MedicareWritableSecond createTestDataSecond(){
        Text codeTreament = new Text();
        //DoubleWritable billRequest = new DoubleWritable();
        
        codeTreament.set("292");
        //billRequest.set(0.18080510831413377);
        
        MedicareWritableSecond dataReducer = new MedicareWritableSecond();
        dataReducer.set(codeTreament);
        
        return dataReducer;
    }

    
    private Text createTestInputSecond(){
        return new Text("292\t0.18080510831413377");
    }
    private Text createTestInputSecond2(){
        return new Text("188\t0.29977");
    }
    private Text createTestInputSecond3(){
        return new Text("222\t10.6");
    }
    private Text createTestInputSecond4(){
        return new Text("04\t7.2");
    }

}
