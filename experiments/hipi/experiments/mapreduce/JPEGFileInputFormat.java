package hipi.experiments.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
/*
	public List<InputSplit> getSplits(JobContext context
	) throws IOException {
		Configuration job = context.getConfiguration();
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(context));
		long maxSize = getMaxSplitSize(context);

	    Path[] dirs = getInputPaths(context);
	    for(Path dir : dirs){
	    	System.out.println("Input path: " + dir.toString());
	    	System.out.println("FS: " + dir.getFileSystem(job).toString());
	    	dir = dir.makeQualified(dir.getFileSystem(job));
	    	System.out.println("Qualified dir: " + dir.toString());
	    }
		
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (FileStatus file: listStatus(context)) {
			Path path = file.getPath();
			System.out.println("Path: " + path.toString());
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if ((length != 0) && isSplitable(context, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining)/splitSize > 1.1) {
					int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
					splits.add(new FileSplit(path, length-bytesRemaining, splitSize,
							blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length-1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
			} else {
				//Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		//LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}	
	*/
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
		System.out.println("Inside add input path");
	    FileSystem fs = path.getFileSystem(conf);//FileSystem.get(conf);
	    path = path.makeQualified(fs);
	    String dirStr = StringUtils.escapeString(path.toString());
	    String dirs = conf.get("mapred.input.dir");
	    conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
	  }
	  
}
