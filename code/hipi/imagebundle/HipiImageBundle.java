package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HipiImageBundle extends AbstractImageBundle {
	
	private static final Log LOG = LogFactory.getLog(HipiImageBundle.class
			.getName());
	
	protected DataInputStream _input_stream = null;
	protected DataOutputStream _output_stream = null;
	
	/**
	 * A set of records where the key is the length of each image and the value
	 * is the image type.
	 * 
	 * TODO: Change from Map to Array of pairs or equiv
	 */
	private Pair<Long, ImageHeader.ImageType> _image_information;
	
	private long _internal_counter = 0;
	
	public HipiImageBundle(Configuration conf) {
		super(conf);
	}

	@Override
	protected void openForWrite(Path output_file) throws IOException {
		// Should never happen because super class ensures this
		if (_output_stream != null || _input_stream != null) {
			throw new IOException("File " + output_file.getName()
					+ " already opened for reading/writing");
		}
		
		// This creates a new file. Since the super class checks for file
		// existence before calling this, and this is a protected method, no
		// user should ever be able to violate the assumption that once you are
		// here you know the file either doesn't exit or should be overwritten.
		_output_stream = new DataOutputStream(FileSystem.get(_conf).create(
				output_file));
	}

	private void readBundleHeader() throws IOException {
		// First 8 bytes is number of files
		Long numFiles = _input_stream.readLong();
		
		// Next is a listing of offsets in the file + their type
		for (int i = 0; i < numFiles; i++) {
			Long offset = _input_stream.readLong();
			ImageType type = ImageType.fromValue(_input_stream.readInt());
			_image_information.put(offset, type);
			
			LOG.info("Found image of length " + offset + " with type " + type);
		}
	}
	
	@Override
	protected void openForRead(Path input_file) throws IOException {
		if (_input_stream != null || _output_stream != null) {
			throw new IOException("File " + input_file.getName()
					+ " already opened for reading/writing");
		}
		
		_input_stream = new DataInputStream(FileSystem.get(_conf).open(
				input_file));
		
		readBundleHeader();
	}

	@Override
	public void addImage(InputStream image_stream, ImageType type)
			throws IOException {
		
	}

	@Override
	public long getImageCount() {
		return _image_information.size();
	}

	@Override
	protected ImageHeader readNextHeader() throws IOException {
		return null;
	}

	@Override
	protected FloatImage readNextImage() throws IOException {
		++_internal_counter;
		return null;
	}

	@Override
	public boolean hasNext() {
		return (_internal_counter < _image_information.size());
	}

	@Override
	public void close() throws IOException {
		if (_input_stream != null) {
			_input_stream.close();
		}
		
		if (_output_stream != null) {
			_output_stream.close();
		}
	}

}
