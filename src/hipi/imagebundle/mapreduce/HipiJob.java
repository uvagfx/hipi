package hipi.imagebundle.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class HipiJob extends Job {

	public HipiJob() throws IOException {
		super();
	}
	
	public HipiJob(Configuration conf) throws IOException {
		super(conf);
	}

	public HipiJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}
	
	public void setDefault(Class<?> cls) throws IllegalStateException {
		setJarByClass(cls);
		setInputFormatClass(ImageBundleInputFormat.class);
	}
}
