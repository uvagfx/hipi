package hipi.imagebundle.mapreduce;

import java.io.IOException;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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

	public void initialize(InputSplit split, JobConf jConf)
			throws IOException, InterruptedException {
		FileSplit bundleSplit = (FileSplit) split;
		conf = jConf;
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
	public ImageHeader createKey() {
		//TODO
		try {
			return reader.getCurrentKey();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public FloatImage createValue() {
		try {
			return reader.getCurrentValue();
		} catch (Exception e) {
			return null;
		}
	}

	@Override //TODO
	public long getPos() throws IOException {
		return 1;
	}

	@Override
	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	@Override
	public boolean next(ImageHeader key, FloatImage value) throws IOException {
		return reader.nextKeyValue();
	}
}
