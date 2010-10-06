package hipi.imagebundle.mapred;

import java.io.IOException;
import java.io.InputStream;

import hipi.excluder.ImageExcluder;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.convert.ImageConverter;

import org.apache.hadoop.io.Writable;
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
public abstract class AbstractImageBundleRecordReader<T extends Writable, U extends ImageExcluder>
		implements RecordReader<T, FloatImage> {
	
	public AbstractImageBundleRecordReader(ImageBundleFileSplit fileSplit) {
		
	}
	
	public void close() throws IOException {
	}

	public abstract T createKey();
	
	public abstract T getNextKey();

	public FloatImage createValue() {		
		return null;
	}

	public long getPos() throws IOException {
		return 0;
	}

	public float getProgress() throws IOException {
		return 0;
	}

	public synchronized boolean next(T key, FloatImage value) throws IOException {	
		key = getNextKey();
		
		return false;
	}
}
