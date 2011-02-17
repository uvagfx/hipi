package hipi.imagebundle.mapreduce;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.HipiImageBundle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class HIBRecordReader extends RecordReader<ImageHeader, FloatImage> {
	private static final Log LOG = LogFactory.getLog(HIBRecordReader.class);


	private HipiImageBundle.FileReader _hib_reader;
	int maxImageSize;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		long start = split.getStart();
		long end = start + split.getLength();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		
		// we need to create some sort of image iterator
		_hib_reader = new HipiImageBundle.FileReader(fs, file, job, start, end);
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		return _hib_reader.getProgress();
	}

	public synchronized void close() throws IOException {
		if (_hib_reader != null) {
			_hib_reader.close();
		}
	}

	@Override
	public ImageHeader getCurrentKey() throws IOException,
			InterruptedException {
		return _hib_reader.getCurrentKey();
	}

	@Override
	public FloatImage getCurrentValue() throws IOException,
			InterruptedException {
		return _hib_reader.getCurrentValue();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return _hib_reader.nextKeyValue();
	}

	
}
