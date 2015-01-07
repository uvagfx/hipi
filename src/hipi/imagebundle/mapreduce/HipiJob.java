package hipi.imagebundle.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class HipiJob {
	public static Job getHipiJobInstance() throws IOException {
		return getHipiJobInstance(new Configuration());
	}
	public static Job getHipiJobInstance(Configuration conf) throws IOException {
		return getHipiJobInstance(conf, "nil");
	}
	public static Job getHipiJobInstance(Configuration conf, String jobName) throws IOException {
		Job job = Job.getInstance();
		job.setInputFormatClass(ImageBundleInputFormat.class);
		return job;
	}
}
