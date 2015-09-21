package org.hipi.imagebundle.mapreduce;

import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.HipiImageBundle;

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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Inherits from {@link FileInputFormat} and processes multiple {@link
 * HipiImageBundle} (HIB) files as input and generates {@link
 * InputSplit} objects for a MapReduce job along with the
 * corresponding {@link RecordReader} class.
 */

public class HibInputFormat extends FileInputFormat<HipiImageHeader, HipiImage> {

  /**
   * Creates a {@link HibRecordReader}
   */
  @Override
  public RecordReader<HipiImageHeader, HipiImage> createRecordReader(InputSplit split,
    TaskAttemptContext context) 
  throws IOException, InterruptedException {
    return new HibRecordReader();
  }

  /**
   * Replacement for non-static protected getBlockIndex which is part
   * of Hadoop and, if used, would prevent computeSplits from being
   * static.
   */
  static protected int staticGetBlockIndex(BlockLocation[] blkLocations, 
					   long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
	  (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
	return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
				       " is outside of file (0.." +
				       fileLength + ")");
  }

  /**
   * Static public method that does all of the heavy lifting of computing InputSplits for a list
   * of HIB files. This is static to allow code reuse: one can imagine many different extensions of
   * ImageBundleInputFormat that produce different record types (raw bytes, UCharImage, FloatImage,
   * OpenCV types, etc.). See, for example, JpegFromHibInputFormat.java.
   */
  static public List<InputSplit> computeSplits(JobContext job, List<FileStatus> inputFiles)
    throws IOException {

    // Read number of requested map tasks from job configuration
    Configuration conf = job.getConfiguration();
    int numMapTasks = conf.getInt("hipi.map.tasks", 0);

    // Initialize list of InputSplits
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Iterate over head input HIB
    for (FileStatus file : inputFiles) {

      // Get path to file and file system object on HDFS
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(conf);
      
      // Create HIB object for reading (pasing null as the image
      // factory disallows calling any of the image reading methods)
      HipiImageBundle hib = new HipiImageBundle(path, conf);
      hib.openForRead();

      // Get image block offsets (should be in ascending order)
      List<Long> offsets = hib.readAllOffsets();
      BlockLocation[] blkLocations = fs.getFileBlockLocations(hib.getDataFileStatus(), 0, offsets.get(offsets.size() - 1));

      if (numMapTasks == 0) {
	// Determine number of map tasks automatically
	int i = 0, b = 0;
	long lastOffset = 0, currentOffset = 0;
	for (; (b < blkLocations.length) && (i < offsets.size()); b++) {
	  long next = blkLocations[b].getOffset() + blkLocations[b].getLength();
	  while (currentOffset < next && i < offsets.size()) {
	    currentOffset = offsets.get(i);
	    i++;
	  }
	  String[] hosts = null;
	  if (currentOffset > next) {
	    Set<String> hostSet = new HashSet<String>();
	    int endIndex = staticGetBlockIndex(blkLocations, currentOffset - 1);
	    for (int j = b; j < endIndex; j++) {
	      String[] blkHosts = blkLocations[j].getHosts();
	      for (int k = 0; k < blkHosts.length; k++)
		hostSet.add(blkHosts[k]);
	    }
	    hosts = (String[]) hostSet.toArray(new String[hostSet.size()]);
	  } else { // currentOffset == next
	    hosts = blkLocations[b].getHosts();
	  }
	  splits.add(new FileSplit(hib.getDataFileStatus().getPath(), lastOffset, currentOffset - lastOffset, hosts));
	  lastOffset = currentOffset;
	}
	System.out.println("Spawned " + b + "map tasks");
      } else {
	// User specified number of map tasks
	int imageRemaining = offsets.size();
	int i = 0, taskRemaining = numMapTasks;
	long lastOffset = 0, currentOffset;
	while (imageRemaining > 0) {
	  int numImages = imageRemaining / taskRemaining;
	  if (imageRemaining % taskRemaining > 0)
	    numImages++;
	  
	  int next = Math.min(offsets.size() - i, numImages) - 1;
	  int startIndex = staticGetBlockIndex(blkLocations, lastOffset);
	  currentOffset = offsets.get(i + next);
	  int endIndex = staticGetBlockIndex(blkLocations, currentOffset - 1);
	  
	  ArrayList<String> hosts = new ArrayList<String>();

	  // Check getBlockIndex, and getBlockSize
	  for (int j = startIndex; j <= endIndex; j++) {
	    String[] blkHosts = blkLocations[j].getHosts();
	    for (int k = 0; k < blkHosts.length; k++)
	      hosts.add(blkHosts[k]);
	  }
	  splits.add(new FileSplit(hib.getDataFileStatus().getPath(), lastOffset, currentOffset - lastOffset, hosts.toArray(new String[hosts.size()])));
	  lastOffset = currentOffset;
	  i += next + 1;
	  taskRemaining--;
	  imageRemaining -= numImages;
	  System.out.println("imageRemaining: " + imageRemaining + "\ttaskRemaining: " + taskRemaining + "\tlastOffset: " + lastOffset + "\ti: " + i);
	}
      }

      // Close HIB
      hib.close();
      
    } // for (FileStatus file : inputFiles)
      
    return splits;
  }

  /**
   * Partitions input HIB files to map tasks in a way that attempts to maximize compute and data
   * co-locality. To this end, {@link InputSplit}s are created such that one map task is created
   * to process the images within one Hadoop block location (multiple images may be in one Hadoop
   * block). The operation of this method is sensitive to the size of data chunks in the Hadoop
   * configuration (smaller data chunks will yield more map tasks, but may also improve cluster
   * utilization). Note that the real work is done in the static public method computeSplits().
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException  {   
    return HibInputFormat.computeSplits(job, listStatus(job));
  }

}
