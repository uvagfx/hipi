package hipi.imagebundle.mapreduce;

import java.io.IOException;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Provides the basic functionality of an ImageBundle record reader. Utilizes 
 * {@link hipi.imagebundle.HipiImageBundle.FileReader} to read the portion of the HipiImageBundle
 * denoted by the InputSplit to get the ImageHeader and FloatImage
 * 
 *
 */
public class ImageBundleRecordReader implements
		RecordReader<ImageHeader, FloatImage> {

	private Configuration conf;
	private HipiImageBundle.FileReader reader;

	public ImageBundleRecordReader(InputSplit split, JobConf jConf) {
		super();
		try {
			initialize(split, jConf);
		} catch (IOException ioe) {
			System.err.println(ioe);
		}
	}

	public void initialize(InputSplit split, JobConf jConf) throws IOException {

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
	public ImageHeader createKey() {
		return new ImageHeader();
	}

	@Override
	public FloatImage createValue() {
		return new FloatImage();
	}

	@Override
	public long getPos() throws IOException {
		return 1;
	}

	@Override
	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	@Override
	public boolean next(ImageHeader key, FloatImage value) throws IOException {

		boolean success = reader.nextKeyValue();

		if(key == null || value == null) {
			System.out.println("key and/or value is null");
			return false;
		}

		key.set(reader.getCurrentKey());
		value.set(reader.getCurrentValue());

		if(success) {
			return true;
		} else {
			return false;
		}
	}
}
