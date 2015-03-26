package hipi.unittest;

import static org.junit.Assert.*;
import hipi.imagebundle.mapreduce.HipiJob;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HipiJobTestCase {

	private HipiJob job;
	
	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();
		job = new HipiJob(conf);
	}

	@Test
	public void testSetMapSpeculativeExecution() {
		job.setMapSpeculativeExecution(false);
		assertEquals(false, job.getConfiguration().getBoolean("mapred.map.tasks.speculative.execution", true));
		
		job.setMapSpeculativeExecution(true);
		assertEquals(true, job.getConfiguration().getBoolean("mapred.map.tasks.speculative.execution", false));
	}

	@Test
	public void testSetReduceSpeculativeExecution() {
		job.setReduceSpeculativeExecution(false);
		assertEquals(false, job.getConfiguration().getBoolean("mapred.reduce.tasks.speculative.execution", true));
		
		job.setReduceSpeculativeExecution(true);
		assertEquals(true, job.getConfiguration().getBoolean("mapred.reduce.tasks.speculative.execution", false));
	}

	@Test
	public void testSetCompressMapOutput() {
		job.setCompressMapOutput(false);
		assertEquals(false, job.getConfiguration().getBoolean("mapred.compress.map.output", true));
		
		job.setCompressMapOutput(true);
		assertEquals(true, job.getConfiguration().getBoolean("mapred.compress.map.output", false));
	}

}
