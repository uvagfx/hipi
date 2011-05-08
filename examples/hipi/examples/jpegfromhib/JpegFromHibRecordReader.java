package hipi.examples.jpegfromhib;

import java.io.IOException;

import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Provides the basic functionality of an ImageBundle record reader's
 * constructor. The constructor is able to read ImageBundleFileSplits and setup
 * the necessary fields to allow the subclass the ability to actually read off
 * FloatImages from the ImageBundle.
 * 
 * This class should be subclassed with a specific Writable object that will
 * serve as the key in the Map tasks.
 * 
 * @author seanarietta
 * 
 * @param <T>
 *            a Writable object that will serve as the key to the Map tasks.
 *            Typically this is either an empty object or the RawImageHeader
 *            object.
 */
public class JpegFromHibRecordReader extends
		RecordReader<NullWritable, BytesWritable> {

	protected Configuration conf;
	private HipiImageBundle.FileReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit bundleSplit = (FileSplit) split;
		conf = context.getConfiguration();
		Path path = bundleSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		// reader specifies start and end, for which start + length would be the beginning of a new file,
		// which is undesirable to reader, -1 must be applied.
		System.out.println("record start from " + bundleSplit.getStart() + " end at " + (bundleSplit.getStart() + bundleSplit.getLength() - 1));
		reader = new HipiImageBundle.FileReader(fs, path, conf,
				bundleSplit.getStart(), bundleSplit.getStart() + bundleSplit.getLength() - 1);
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return new BytesWritable(reader.getRawBytes());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}
}
