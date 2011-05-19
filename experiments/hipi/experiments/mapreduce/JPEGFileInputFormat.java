package hipi.experiments.mapreduce;

import java.io.IOException;
import java.util.List;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

public class JPEGFileInputFormat extends
FileInputFormat<ImageHeader, FloatImage> {

	@Override
	public RecordReader<ImageHeader, FloatImage> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new JPEGRecordReader();
	}

	/** Splits files returned by {@link #listStatus(Configuration)} when
	 * they're too big.*/

	public List<InputSplit> getSplits(JobContext context
	) throws IOException {
		long startT=0;
		long stopT=0;	   
		startT = System.currentTimeMillis();
		List<InputSplit> splits = super.getSplits(context);
		stopT = System.currentTimeMillis();
		float el = (float)(stopT-startT)/1000.0f;
		System.out.println("Total Time: " + el);
		return splits;
		
	}	
	
	/**
	   * Add a {@link Path} to the list of inputs for the map-reduce job.
	   *
	   * @param conf The configuration of the job
	   * @param path {@link Path} to be added to the list of inputs for
	   *            the map-reduce job.
	   */
	public static void addInputPath(Job job,
	                                  Path path) throws IOException {
		Configuration conf = job.getConfiguration();
	    FileSystem fs = path.getFileSystem(conf);
	    path = path.makeQualified(fs);
	    String dirStr = StringUtils.escapeString(path.toString());
	    String dirs = conf.get("mapred.input.dir");
	    conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
	  }
	  
}
