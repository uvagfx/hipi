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

	protected DataInputStream _index_input_stream = null;
	protected DataOutputStream _index_output_stream = null;
	protected DataInputStream _data_input_stream = null;
	protected DataOutputStream _data_output_stream = null;
	protected BufferedInputStream _buffered_data_input_stream = null;

	/**
	 * A set of records where the key is the length of each image and the value
	 * is the image type.
	 * 
	 * TODO: Change from Map to Array of pairs or equiv
	 */
	private byte _sig[] = new byte[8];
	private int _cacheLength = 0;
	private int _cacheType = 0;
	private long _countingOffset = 0;
	private Path _index_file = null;
	private Path _data_file = null;
	private long _imageCount = -1;

	public HipiImageBundle(Configuration conf) {
		super(conf);
	}

	private void writeBundleHeader() throws IOException {
		// the index file header designed like this:
		// 4-byte magic signature (0x81911618) for "HIPIIbIH"
		// 2-byte (short int) to denote the length of data file name
		// variable bytes of data file name
		// 8-byte of image count (not mandated)
		// 16-byte of reserved field
		// 4-byte points to how much to skip in order to reach the start of
		// offsets (default 0)
		// 8-byte of offsets starts from here until EOF
		_index_output_stream.writeInt(0x81911b18);
		String data_name = _data_file.getName();
		// write out filename in UTF-8 encoding
		byte[] name_byte = data_name.getBytes("UTF-8");
		_index_output_stream.writeShort(name_byte.length);
		_index_output_stream.write(name_byte);
		// write out image count (default -1 (unknown count))
		_index_output_stream.writeLong(-1);
		// write out reserved field
		_index_output_stream.writeLong(0);
		_index_output_stream.writeLong(0);
		// skip 0 to reach start offset (potentially, you could put some
		// metadata in between
		_index_output_stream.writeInt(0);
	}

	@Override
	protected void openForWrite(Path output_file) throws IOException {
		// Check if the instance is already in some read/write states
		if (_data_output_stream != null || _data_input_stream != null
				|| _index_output_stream != null || _index_input_stream != null) {
			throw new IOException("File " + output_file.getName()
					+ " already opened for reading/writing");
		}

		// HipiImageBundle will create two files: an index file and a data file
		// by default, the Path of output_file is the Path of index file, and
		// data
		// file will simply append .dat suffix
		_index_file = output_file;
		_index_output_stream = new DataOutputStream(FileSystem.get(_conf)
				.create(_index_file));
		_data_file = output_file.suffix(".dat");
		_data_output_stream = new DataOutputStream(FileSystem.get(_conf)
				.create(_data_file));
		_countingOffset = 0;
		writeBundleHeader();
	}

	private void readBundleHeader() throws IOException {
		// check the signature in head
		int sig = _index_input_stream.readInt();
		if (sig != 0x81911b18) {
			throw new IOException("not a hipi image bundle");
		}
		short name_len = _index_input_stream.readShort();
		byte[] name_byte = new byte[name_len];
		_index_input_stream.read(name_byte);
		String data_name = new String(name_byte, "UTF-8");
		_data_file = new Path(_index_file.getParent(), data_name);
		_imageCount = _index_input_stream.readLong();
		// using readLong instead of skip because skip doesn't insure you can
		// actually skip that much, and if readLong reached the EOF, will throw
		// error anyway
		_index_input_stream.readLong();
		_index_input_stream.readLong();
		int skipOver = _index_input_stream.readInt();
		while (skipOver > 0) {
			long skipped = _index_input_stream.skip(skipOver);
			if (skipped <= 0)
				break;
			skipOver -= skipped;
		}
		_cacheLength = _cacheType = 0;
	}

	@Override
	protected void openForRead(Path input_file) throws IOException {
		if (_data_output_stream != null || _data_input_stream != null
				|| _index_output_stream != null || _index_input_stream != null) {
			throw new IOException("File " + input_file.getName()
					+ " already opened for reading/writing");
		}

		_index_file = input_file;
		_index_input_stream = new DataInputStream(FileSystem.get(_conf).open(
				_index_file));

		readBundleHeader();

		_data_input_stream = new DataInputStream(FileSystem.get(_conf).open(
				_data_file));
		_buffered_data_input_stream = new BufferedInputStream(
				_data_input_stream);
	}

	@Override
	public void addImage(InputStream image_stream, ImageType type)
			throws IOException {
		byte data[] = new byte[image_stream.available()];
		image_stream.read(data);
		_cacheLength = data.length;
		_cacheType = type.toValue();
		_sig[0] = (byte) (_cacheLength >> 24);
		_sig[1] = (byte) ((_cacheLength >> 16) & 0xff);
		_sig[2] = (byte) ((_cacheLength >> 8) & 0xff);
		_sig[3] = (byte) (_cacheLength & 0xff);
		_sig[4] = (byte) (_cacheType >> 24);
		_sig[5] = (byte) ((_cacheType >> 16) & 0xff);
		_sig[6] = (byte) ((_cacheType >> 8) & 0xff);
		_sig[7] = (byte) (_cacheType & 0xff);
		_data_output_stream.write(_sig);
		_data_output_stream.write(data);
		_index_output_stream.writeLong(_countingOffset);
		_countingOffset += 8 + data.length;
	}

	@Override
	public long getImageCount() {
		return _imageCount;
	}

	@Override
	protected ImageHeader readHeader() throws IOException {
		if (_cacheLength > 0) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType
					.fromValue(_cacheType));
			if (decoder == null)
				return null;
			ImageHeader header = decoder
					.decodeImageHeader(_buffered_data_input_stream);
			_buffered_data_input_stream.reset();
			return header;
		}
		return null;
	}

	@Override
	protected FloatImage readImage() throws IOException {
		if (_cacheLength > 0) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType
					.fromValue(_cacheType));
			if (decoder == null)
				return null;
			FloatImage image = decoder.decodeImage(_buffered_data_input_stream);
			_buffered_data_input_stream.reset();
			return image;
		}
		return null;
	}

	@Override
	protected boolean prepareNext() {
		try {
			while (_cacheLength > 0) {
				long skipped = _buffered_data_input_stream
						.skip((long) _cacheLength);
				if (skipped <= 0)
					break;
				_cacheLength -= skipped;
			}
			int byteRead = _buffered_data_input_stream.read(_sig);
			if (byteRead <= 0)
				return false;
			_cacheLength = ((_sig[0] & 0xff) << 24) | ((_sig[1] & 0xff) << 16)
					| ((_sig[2] & 0xff) << 8) | (_sig[3] & 0xff);
			_cacheType = ((_sig[4] & 0xff) << 24) | ((_sig[5] & 0xff) << 16)
					| ((_sig[6] & 0xff) << 8) | (_sig[7] & 0xff);
			_buffered_data_input_stream.mark(_cacheLength);
			return true;
		} catch (IOException e) {
			LOG.info("reach the end of the file");
			return false;
		}
	}

	@Override
	public void merge(AbstractImageBundle[] bundles) {

	}

	@Override
	public void close() throws IOException {
		if (_buffered_data_input_stream != null) {
			_buffered_data_input_stream.close();
		}

		if (_data_input_stream != null) {
			_data_input_stream.close();
		}

		if (_index_input_stream != null) {
			_index_input_stream.close();
		}

		if (_data_output_stream != null) {
			_data_output_stream.close();
		}

		if (_index_output_stream != null) {
			_index_output_stream.close();
		}
	}

}
