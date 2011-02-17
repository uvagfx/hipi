package hipi.imagebundle.mapreduce;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HIBInputFormat extends FileInputFormat<ImageHeader, FloatImage> {

	public static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	//generate input splits from the src file lists
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(context));
		long maxSize = getMaxSplitSize(context);
		

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		
		for (FileStatus file: listStatus(context)) {
			System.out.println("File for input split: " + file.getPath());
			Path index_file = file.getPath();
		
			HipiImageBundle hib = new HipiImageBundle(context.getConfiguration());
			hib.open(index_file, AbstractImageBundle.FILE_MODE_READ);
			List<Long> offsets = hib.getOffsets();
			
						
			//now get info about data file
			FileStatus dataFile = hib.getDataFile();
			Path dataPath = dataFile.getPath();
			FileSystem fs = dataPath.getFileSystem(context.getConfiguration());
			long data_length = dataFile.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(dataFile, 0, data_length);
			if ((data_length != 0) && isSplitable(context, dataPath)) {
				long blockSize = dataFile.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = data_length;
				long startOffset = 0;
				Iterator<Long> it = offsets.iterator();
				while (bytesRemaining > splitSize){
					long endOffset = startOffset;
					//loop until the candidate split is an ideal size
					while(it.hasNext() && ((endOffset = it.next()) - startOffset < splitSize)){};
					
					int blkIndex = getBlockIndex(blkLocations, startOffset);
					splits.add(new FileSplit(dataPath, startOffset, endOffset - startOffset,
							blkLocations[blkIndex].getHosts()));
					bytesRemaining -= (endOffset - startOffset);
					startOffset = endOffset;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(dataPath, data_length - bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length-1].getHosts()));
				}
			} else if (data_length != 0) {
				splits.add(new FileSplit(dataPath, 0, data_length, blkLocations[0].getHosts()));
			} else {
				//Create empty hosts array for zero length files
				splits.add(new FileSplit(dataPath, 0, data_length, new String[0]));
			}
		}
		LOG.debug("Total # of splits: " + splits.size());
		return splits;

	}


	@Override
	public RecordReader<ImageHeader, FloatImage> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new HIBRecordReader();
	}

}