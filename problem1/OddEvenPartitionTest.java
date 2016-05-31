package finalproject.problem1;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OddEvenPartitionTest {

	static String[] months = {"Odd","Even"};
	OddEvenPartitioner<MedicareWritable, IntWritable> mpart;

	@Test
	public void testMonthPartition() {

		mpart=new OddEvenPartitioner<MedicareWritable, IntWritable>();
		mpart.setConf(new Configuration());
//		int result;
//		for (int i = 0; i < months.length; i++) {
//			result = mpart.getPartition(createTestData(), new IntWritable(1), 2);
//			assertEquals(result,i);			
//		}
		
	}
	private MedicareWritable createTestData() {
		  IntWritable reqno = new IntWritable();
			Text url = new Text();
			Text rdate = new Text();
			Text rtime = new Text();
			Text rip = new Text();
			
			reqno.set(168);
			url.set("/assets/img/search-button.gif"); 
			rdate.set("29/Aug/2009");
			rtime.set("14:17:54");
			rip.set("10.211.47.159"); 
			
		    MedicareWritable dataReducer = new MedicareWritable();
		    //dataReducer.set(reqno, url, rdate, rtime, rip);
		    return dataReducer;
	  }

}
