package hipi.unittest;

import static org.junit.Assert.*;

import hipi.imagebundle.mapreduce.HipiJob;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.junit.Before;
import org.junit.Test;

public class HipiJobTestCase {

	private Job job;
	
	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();
		job = HipiJob.getHipiJobInstance(conf);
	}

	//TODO - how to test class
	@Test
	public void testSetInputFormatClass() {
		// try {
		// 	assertTrue(job.getInputFormatClass() instanceof ImageBundleInputFormat); 
		// } catch (ClassNotFoundException cnfe) {
		// 	cnfe.printStackTrace();
		// 	fail();
		// }
	}

}
