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
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;



/**
 * With one or many HipiImageBundles as an input, ImageBundleInputFormat will generate InputSplits for the MapReduce
 * tasks and create the corresponding RecordReaders.  
 *
 */

public class ImageBundleInputFormat extends
		FileInputFormat <ImageHeader, FloatImage> {

	/**
	 * Creates an {@link ImageBundleRecordReader}
	 */
	@Override
	public RecordReader<ImageHeader, FloatImage> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new ImageBundleRecordReader();
	}

	/**
	 * Splits the input to Map tasks to maximize data locality when the Mappers are being run. As such, 
	 * {@link InputSplit}s are created such that one Map Node is created for each data chunk to ensure locality.
	 * Multiple images may be on one data chunk. This method is very sensitive to Hadoop's setup for the size of 
	 * data chunks (smaller data chunks yield more map tasks).
	 */
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		System.out.println("~~~~~~~~~~~~~~~ IN GET SPLIT !~~~~~~~~~~~~~~");
		int numMapTasks = job.getInt("hipi.map.tasks", 0);
		List<FileSplit> splits = new ArrayList<FileSplit>();
		for (FileStatus file : listStatus(job)) {
			Path path = file.getPath();
			System.out.println("Hib path: " + path.toString());
			FileSystem fs = path.getFileSystem(job);
			HipiImageBundle hib = new HipiImageBundle(path, job);
			hib.open(AbstractImageBundle.FILE_MODE_READ);
			// offset should be guaranteed to be in order
			System.out.println("offsets data: "+hib.getOffsets().size());
			List<Long> offsets = hib.getOffsets();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(hib.getDataFile(), 0, offsets.get(offsets.size() - 1));
			if (numMapTasks == 0) {
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
						int endIndex = getBlockIndex(blkLocations, currentOffset - 1);
						for (int j = b; j < endIndex; j++) {
							String[] blkHosts = blkLocations[j].getHosts();
							for (int k = 0; k < blkHosts.length; k++)
								hostSet.add(blkHosts[k]);
						}
						hosts = (String[]) hostSet.toArray(new String[hostSet.size()]);
					} else { // currentOffset == next
						hosts = blkLocations[b].getHosts();
					}
					splits.add(new FileSplit(hib.getDataFile().getPath(), lastOffset, currentOffset - lastOffset, hosts));
					lastOffset = currentOffset;
				}
				System.out.println(b + " tasks spawned");
			} else {
				int imageRemaining = offsets.size();
				int i = 0, taskRemaining = numMapTasks;
				long lastOffset = 0, currentOffset;
				while (imageRemaining > 0) {
					int numImages = imageRemaining / taskRemaining;
					if (imageRemaining % taskRemaining > 0)
						numImages++;
					int next = Math.min(offsets.size() - i, numImages) - 1;
					int startIndex = getBlockIndex(blkLocations, lastOffset);
					currentOffset = offsets.get(i + next);
					int endIndex = getBlockIndex(blkLocations, currentOffset - 1);
					ArrayList<String> hosts = new ArrayList<String>();
					// check getBlockIndex, and getBlockSize
					for (int j = startIndex; j <= endIndex; j++) {
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
		}
		FileSplit[] splitsArray = new FileSplit[splits.size()];
		System.out.println("size: "+splits.size());
		for(int i = 0; i < splits.size(); i++) {
			splitsArray[i] = splits.get(i);
		}

		return splitsArray;
	}
}
