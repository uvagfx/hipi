package hipi.imagebundle.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImageBundleInputFormat extends
		FileInputFormat<ImageHeader, FloatImage> {

	@Override
	public RecordReader<ImageHeader, FloatImage> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ImageBundleRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		int numMapTasks = conf.getInt("hipi.map.tasks", 1);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (FileStatus file : listStatus(job)) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(conf);
			HipiImageBundle hib = new HipiImageBundle(conf);
			hib.open(path, AbstractImageBundle.FILE_MODE_READ);
			// offset should be guaranteed to be in order
			List<Long> offsets = hib.getOffsets();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(hib.getDataFile(), 0, offsets.get(offsets.size() - 1));
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
				for (int j = startIndex; j <= endIndex; j++) {
					String[] blkHosts = blkLocations[j].getHosts();
					for (int k = 0; k < blkHosts.length; k++)
						hosts.add(blkHosts[k]);
				}
				splits.add(new FileSplit(hib.getDataFile().getPath(), lastOffset, currentOffset, hosts.toArray(new String[hosts.size()])));
				lastOffset = currentOffset;
				i += next + 1;
				taskRemaining--;
				imageRemaining -= numImages;
				System.out.println("imageRemaining: " + imageRemaining + "\ttaskRemaining: " + taskRemaining + "\tlastOffset: " + lastOffset + "\ti: " + i);
			}
			hib.close();
		}
		return splits;
	}
}
