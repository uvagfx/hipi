package hipi.imagebundle.mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 * With one or many HipiImageBundles as an input, ImageBundleInputFormat will generate InputSplits
 * for the MapReduce tasks and create the corresponding RecordReaders.
 */

public class ImageBundleInputFormat extends FileInputFormat<ImageHeader, FloatImage> {

  /**
   * Creates an {@link ImageBundleRecordReader}
   */
  @Override
  public RecordReader<ImageHeader, FloatImage> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException 
  {
      return new ImageBundleRecordReader();
  }

  /**
   * Divides the input to Map tasks to maximize data locality when the Mappers are being run. As
   * such, {@link InputSplit}s are created such that one Map Node is created for each data chunk to
   * ensure locality. Multiple images may be on one data chunk. This method is very sensitive to
   * Hadoop's setup for the size of data chunks (smaller data chunks yield more map tasks).
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException 
  {
      // read number of requested map tasks from job configuration
      Configuration conf = job.getConfiguration();
      int numMapTasks = conf.getInt("hipi.map.tasks", 0);

      // initialize list of InputSplits
      List<InputSplit> splits = new ArrayList<InputSplit>();

      for (FileStatus file : listStatus(job)) // for each HIB file...
	  {
	      // obtain path to file and file system object on HDFS
	      Path path = file.getPath();
	      FileSystem fs = path.getFileSystem(conf);

	      // create HIB object for reading
	      HipiImageBundle hib = new HipiImageBundle(path, conf);
	      hib.open(AbstractImageBundle.FILE_MODE_READ);

	      // get JPEG block offsets (should be in ascending order)
	      List<Long> offsets = hib.getOffsets();
	      BlockLocation[] blkLocations = fs.getFileBlockLocations(hib.getDataFile(), 0, offsets.get(offsets.size() - 1));

	      if (numMapTasks == 0) // determine number of map tasks automatically
		  {
		      int i = 0, b = 0;
		      long lastOffset = 0, currentOffset = 0;
		      for (; (b < blkLocations.length) && (i < offsets.size()); b++) 
			  {
			      long next = blkLocations[b].getOffset() + blkLocations[b].getLength();
			      while (currentOffset < next && i < offsets.size()) 
				  {
				      currentOffset = offsets.get(i);
				      i++;
				  }
			      String[] hosts = null;
			      if (currentOffset > next) 
				  {
				      Set<String> hostSet = new HashSet<String>();
				      int endIndex = getBlockIndex(blkLocations, currentOffset - 1);
				      for (int j = b; j < endIndex; j++) 
					  {
					      String[] blkHosts = blkLocations[j].getHosts();
					      for (int k = 0; k < blkHosts.length; k++)
						  hostSet.add(blkHosts[k]);
					  }
				      hosts = (String[]) hostSet.toArray(new String[hostSet.size()]);
				  }
			      else
				  { // currentOffset == next
				      hosts = blkLocations[b].getHosts();
				  }
			      splits.add(new FileSplit(hib.getDataFile().getPath(), lastOffset, currentOffset - lastOffset, hosts));
			      lastOffset = currentOffset;
			  }
		      System.out.println(b + " tasks spawned");
		  }
	      else // user specified number of tasks
		  {
		      int imageRemaining = offsets.size();
		      int i = 0, taskRemaining = numMapTasks;
		      long lastOffset = 0, currentOffset;
		      while (imageRemaining > 0) 
			  {
			      int numImages = imageRemaining / taskRemaining;
			      if (imageRemaining % taskRemaining > 0)
				  numImages++;

			      int next = Math.min(offsets.size() - i, numImages) - 1;
			      int startIndex = getBlockIndex(blkLocations, lastOffset);
			      currentOffset = offsets.get(i + next);
			      int endIndex = getBlockIndex(blkLocations, currentOffset - 1);

			      ArrayList<String> hosts = new ArrayList<String>();
			      // check getBlockIndex, and getBlockSize
			      for (int j = startIndex; j <= endIndex; j++) 
				  {
				      String[] blkHosts = blkLocations[j].getHosts();
				      for (int k = 0; k < blkHosts.length; k++)
					  hosts.add(blkHosts[k]);
				  }
			      splits.add(new FileSplit(hib.getDataFile().getPath(), lastOffset, currentOffset - lastOffset, hosts.toArray(new String[hosts.size()])));
			      lastOffset = currentOffset;
			      i += next + 1;
			      taskRemaining--;
			      imageRemaining -= numImages;
			      System.out.println("imageRemaining: " + imageRemaining + "\ttaskRemaining: " + taskRemaining + "\tlastOffset: " + lastOffset + "\ti: " + i);
			  }
		  }
	      
	      hib.close();

	  } // for (FileStatus file : listStatus(job)) 
      
      return splits;
  }

}
