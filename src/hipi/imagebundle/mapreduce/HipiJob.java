package hipi.imagebundle.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * This class extends the normal {@link org.apache.hadoop.mapreduce.Job} class defined 
 * in Hadoop to allow a more intuitive configuration of the Job.
 *
 */
public class HipiJob extends Job {

	public HipiJob() throws IOException {
		this(new Configuration());
	}
	
	public HipiJob(Configuration conf) throws IOException {
		this(conf, "nil");
	}
	
	/**
	 * This constructor will set some default parameters of the MapReduce job including:
	 * <ul>
	 * <li> Sets the InputFormat class to {@link hipi.imagebundle.mapreduce.ImageBundleInputFormat}</li>
	 * </ul>
	 * @param conf a Configuration whose settings will be inherited
	 * @param jobName the name of the job as it will appear in the task tracker
	 * @throws IOException
	 */
	public HipiJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
		// Set some default behavior that works for most jobs in HIPI
		this.setInputFormatClass(ImageBundleInputFormat.class);
	}

	/**
	 * Turn speculative execution on or off for this job for map tasks.
	 * @param speculativeExecution {@value true} if speculative execution should be turned on for map tasks, else {@value false}.
	 */
	public void setMapSpeculativeExecution(boolean speculativeExecution) {		
		this.conf.setBoolean("mapred.map.tasks.speculative.execution", speculativeExecution);		
	}
	
	/**
	 * Turn speculative execution on or off for this job for reduce tasks.
	 * @param speculativeExecution {@value true} if speculative execution should be turned on for map tasks, else {@value false}.
	 */
	public void setReduceSpeculativeExecution(boolean speculativeExecution) {
		this.conf.setBoolean("mapred.reduce.tasks.speculative.execution", speculativeExecution);
	}
	
	/**
	 * Enable compression of map output records before they are sent to the reduce phase.
	 * @param compressOutput {@value true} if compression should be enabled, else {@value false}.
	 */
	public void setCompressMapOutput(boolean compressOutput) {
		this.conf.setBoolean("mapred.compress.map.output", compressOutput);
		this.conf.set("mapred.output.compression.type", "BLOCK");
	}
}
