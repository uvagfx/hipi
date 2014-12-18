package hipi.examples.jpegfromhib;

import java.io.IOException;

import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

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
public class JpegFromHibRecordReader implements
		RecordReader<NullWritable, BytesWritable> {

	protected Configuration conf;
	private HipiImageBundle.FileReader reader;

	public JpegFromHibRecordReader(InputSplit split, JobConf jConf) {
		try {
			initialize(split, jConf);
		} catch (IOException ioe) {
			System.err.println(ioe);
		}
	}


	private void initialize(InputSplit split, JobConf jConf) throws IOException {
		FileSplit bundleSplit = (FileSplit) split;
		conf = jConf;
		Path path = bundleSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		// reader specifies start and end, for which start + length would be the beginning of a new 
		// file, which is undesirable to reader, -1 must be applied.
		System.out.println("record start from " + bundleSplit.getStart() + " end at " + 
			(bundleSplit.getStart() + bundleSplit.getLength() - 1));
		reader = new HipiImageBundle.FileReader(fs, path, conf,
				bundleSplit.getStart(), bundleSplit.getStart() + bundleSplit.getLength() - 1);
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	@Override
	public long getPos(){
		return 1; //TODO
	}

	@Override
	public boolean next(NullWritable key, BytesWritable value) throws IOException {
		if (reader.nextKeyValue()) {
			BytesWritable imageData = new BytesWritable(reader.getRawBytes());
			value.set(imageData);
			return true;
		} else {
			return false;
		}

	}
}
