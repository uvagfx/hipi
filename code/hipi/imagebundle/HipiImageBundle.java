package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

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
	protected BufferedInputStream _buffered_input_stream = null;
	
	/**
	 * A set of records where the key is the length of each image and the value
	 * is the image type.
	 * 
	 * TODO: Change from Map to Array of pairs or equiv
	 */
	private byte _sig[] = new byte[8];
	private int _cacheLength = 0;
	private int _cacheType = 0;
	
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
		byte sig[] = {'H', 'I', 'P', 'I'};
		_output_stream.write(sig);
	}

	private void readBundleHeader() throws IOException {
		// check the signature in head
		byte sig[] = new byte[4];
		_buffered_input_stream.read(sig);
		if (sig[0] != 'H' || sig[1] != 'I' || sig[2] != 'P' || sig[3] != 'I') {
			throw new IOException("not a hipi image bundle");
		}
		_cacheLength = _cacheType = 0;
	}
	
	@Override
	protected void openForRead(Path input_file) throws IOException {
		if (_input_stream != null || _output_stream != null) {
			throw new IOException("File " + input_file.getName()
					+ " already opened for reading/writing");
		}
		
		_input_stream = new DataInputStream(FileSystem.get(_conf).open(
				input_file));
		_buffered_input_stream = new BufferedInputStream(_input_stream);

		readBundleHeader();
	}

	@Override
	public void addImage(InputStream image_stream, ImageType type)
			throws IOException {
		byte data[] = new byte[image_stream.available()];
		image_stream.read(data);
		_cacheLength = data.length;
		_cacheType = type.toValue();
		_sig[0] = (byte)(_cacheLength >> 24);
		_sig[1] = (byte)((_cacheLength >> 16) & 0xff);
		_sig[2] = (byte)((_cacheLength >> 8) & 0xff);
		_sig[3] = (byte)(_cacheLength & 0xff);
		_sig[4] = (byte)(_cacheType >> 24);
		_sig[5] = (byte)((_cacheType >> 16) & 0xff);
		_sig[6] = (byte)((_cacheType >> 8) & 0xff);
		_sig[7] = (byte)(_cacheType & 0xff);
		_output_stream.write(_sig);
		_output_stream.write(data);
	}

	@Override
	public long getImageCount() {
		return 0;
	}

	@Override
	protected ImageHeader readNextHeader() throws IOException {
		while (_cacheLength > 0) {
			long skipped = _buffered_input_stream.skip((long) _cacheLength);
			if (skipped == 0)
				break;
			_cacheLength -= skipped;
		}
		_buffered_input_stream.read(_sig);
		_cacheLength = ((_sig[0] & 0xff) << 24) | ((_sig[1] & 0xff) << 16) | ((_sig[2] & 0xff) << 8) | (_sig[3] & 0xff);
		_cacheType = ((_sig[4] & 0xff) << 24) | ((_sig[5] & 0xff) << 16) | ((_sig[6] & 0xff) << 8) | (_sig[7] & 0xff);
		ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
		if (decoder == null)
			return null;
		_buffered_input_stream.mark(_cacheLength);
		ImageHeader header = decoder.decodeImageHeader(_buffered_input_stream);
		_buffered_input_stream.reset();
		return header;
	}

	@Override
	protected FloatImage readNextImage() throws IOException {
		ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
		if (decoder == null)
			return null;
		FloatImage image = decoder.decodeImage(_buffered_input_stream);
		_cacheLength = _cacheType = 0;
		return image;
	}

	@Override
	public boolean hasNext() {
		try {
			while (_cacheLength > 0) {
				long skipped = _buffered_input_stream.skip((long) _cacheLength);
				if (skipped == 0)
					break;
				_cacheLength -= skipped;
			}
			_buffered_input_stream.mark(8);
			_buffered_input_stream.read(_sig);
			_buffered_input_stream.reset();
			_cacheLength = _cacheType = 0;
			return (_sig[0] != 0 || _sig[1] != 0 || _sig[2] != 0 || _sig[3] != 0 ||
					_sig[4] != 0 || _sig[5] != 0 || _sig[6] != 0 || _sig[7] != 0);
		} catch (IOException e) {
			LOG.info("reach the end of the file");
			return false;
		}
	}

	@Override
	public void close() throws IOException {
		if (_buffered_input_stream != null) {
			_buffered_input_stream.close();
		}

		if (_input_stream != null) {
			_input_stream.close();
		}
		if (_output_stream != null) {
			_sig[0] = _sig[1] = _sig[2] = _sig[3] = _sig[4] = _sig[5] = _sig[6] = _sig[7] = 0;
			_output_stream.write(_sig);
			_output_stream.close();
		}
	}

}
