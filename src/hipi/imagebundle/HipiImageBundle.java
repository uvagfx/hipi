package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HipiImageBundle is HIPI's main way of storing collections of images. It takes advantage of the fact that
 * Hadoop's MapReduce works best with data locality and large files as opposed to small files. This class provides
 * methods for easily writing and reading to/from a HipiImageBundle. HipiImageBundles are broken up into an index file
 * containing image offsets and a data file.
 * The full implementation can be found at HIPI's main website.
 * 
 * @see <a href="http://hipi.cs.virginia.edu/">HIPI Project Homepage</a>
 *
 */
public class HipiImageBundle extends AbstractImageBundle {

	/**

	 * The reader class provides a simple interface for reading images from a {@link hipi.imagebundle.HipiImageBundle} so that
	 * the HipiImageBundle can be split into multiple sections and be read in parallel. This 
	 * It is used heavily by {@link hipi.imagebundle.mapreduce.ImageBundleInputFormat} 
	 * and {@link hipi.imagebundle.mapreduce.ImageBundleRecordReader} for MapReduce jobs because of this parallelism.
	 *
	 */
	public static class FileReader {

		private DataInputStream _data_input_stream = null;

		private byte _sig[] = new byte[8];
		private int _cacheLength = 0;
		private int _cacheType = 0;
		private long _countingOffset = 0;
		private long _start = 0;
		private long _end = 0;
		private ImageHeader _header;
		private FloatImage _image;
		private byte[] _byte_array_data;

		/**
		 * 
		 * @return The progress of reading through the HipiImageBundle according to the start and end specified in the constructor
		 */
		public float getProgress() {
			return (_end - _start) > 0 ? (float)(_countingOffset - _start) / (_end - _start) : 0;
		}

		/**
		 * 
		 * @param fs The {@link FileSystem} where the HipiImageBundle resides
		 * @param path The {@link Path} to the HipiImageBundle
		 * @param conf {@link Configuration} for the HipiImageBundle
		 * @param start The offset position to start reading the HipiImageBundle
		 * @param end The offset position to stop reading the HipiImageBundle
		 * @throws IOException
		 */
		public FileReader(FileSystem fs, Path path, Configuration conf,
				long start, long end) throws IOException {
			_data_input_stream = new DataInputStream(fs.open(path));
			_start = start;
			while (start > 0) {
				long skipped = _data_input_stream
				.skip((long) start);
				if (skipped <= 0)
					break;
				start -= skipped;
			}
			_countingOffset = _start;
			_end = end;
		}

		public void close() throws IOException {
			if (_data_input_stream != null)
				_data_input_stream.close();
		}

		/**
		 * Reads the next image in the HipiImageBundle into a cache. Images are not decoded in this method. To get
		 * the corresponding {@link FloatImage} and {@link ImageHeader} you must call {@link #getCurrentValue()} and
		 * {@link #getCurrentKey()} respectively.
		 * 
		 * @return True if the reader could get the next image from the HipiImageBundle. False if there are no more images
		 * or an error occurred.
		 */
		public boolean nextKeyValue() {
			try {
				if (_end > 0 && _countingOffset > _end) {
					_cacheLength = _cacheType = 0;
					return false;
				}

				int readOff = 0;
				int byteRead = _data_input_stream.read(_sig);
				// even only 8-byte, it requires to retry
				while (byteRead < 8 - readOff && byteRead > 0) {
					readOff += byteRead;
					byteRead = _data_input_stream.read(_sig, readOff, 8 - readOff);
				}
				if (byteRead <= 0) {
					_cacheLength = _cacheType = 0;
					return false;
				}
				if (byteRead < 8)
					System.out.println("lacking of " + byteRead);
				_cacheLength = ((_sig[0] & 0xff) << 24)
				| ((_sig[1] & 0xff) << 16) | ((_sig[2] & 0xff) << 8)
				| (_sig[3] & 0xff);
				_cacheType = ((_sig[4] & 0xff) << 24)
				| ((_sig[5] & 0xff) << 16) | ((_sig[6] & 0xff) << 8)
				| (_sig[7] & 0xff);

				_image = null;
				_header = null;
				if (_cacheLength < 0)
				{
					System.out.println("corrupted HipiImageBundle at offset: " + _countingOffset + ", exiting ...");
					_cacheLength = _cacheType = 0;
					return false;
				}
				_byte_array_data = new byte[_cacheLength];
				readOff = 0;
				// it may requires several round-trip in order for this to work
				byteRead = _data_input_stream.read(_byte_array_data);
				while (byteRead < _byte_array_data.length - readOff && byteRead > 0) {
					readOff += byteRead;
					byteRead = _data_input_stream.read(_byte_array_data, readOff, _byte_array_data.length - readOff);
				}
				if (byteRead <= 0) {
					_cacheLength = _cacheType = 0;
					return false;
				}
				// the total skip is cache length plus 8 (4 for cache length, 4 for cache type)
				_countingOffset += _cacheLength + 8;
				return true;
			} catch (IOException e) {
				return false;
			}
		}

		/**
		 * 
		 * @return Raw byte array containing the image as stored in the HipiImageBundle. The image will not be decoded
		 * into an {@link FloatImage}.
		 * 
		 * @throws IOException
		 */
		public byte[] getRawBytes() throws IOException {
			if (_cacheLength > 0) {
				return _byte_array_data;
			}
			return null;
		}

		/**
		 * 
		 * @return ImageHeader of the current image, as retrieved by {@link #nextKeyValue()}
		 * @throws IOException
		 */
		public ImageHeader getCurrentKey() throws IOException {
			if (_header != null)
				return _header;
			if (_cacheLength > 0) {
				ImageDecoder decoder = CodecManager.getDecoder(ImageType
						.fromValue(_cacheType));
				if (decoder == null)
					return null;
				ByteArrayInputStream _byte_array_input_stream = new ByteArrayInputStream(_byte_array_data);
				try {
					_header = decoder.decodeImageHeader(_byte_array_input_stream);
				} catch (Exception e) {
					e.printStackTrace();
					_header = null;
				}
				return _header;
			}
			return null;
		}

		/**
		 * 
		 * @return Decoded image as a {@link FloatImage}, as retrieved by {@link #nextKeyValue()}
		 * @throws IOException
		 */
		public FloatImage getCurrentValue() throws IOException {
			if (_image != null)
				return _image;
			if (_cacheLength > 0) {
				ImageDecoder decoder = CodecManager.getDecoder(ImageType
						.fromValue(_cacheType));
				if (decoder == null)
					return null;
				ByteArrayInputStream _byte_array_input_stream = new ByteArrayInputStream(_byte_array_data);
				try {
					_image = decoder.decodeImage(_byte_array_input_stream);
				} catch (Exception e) {
					e.printStackTrace();
					_image = null;
				}
				return _image;
			}
			return null;
		}

	}

	private DataInputStream _index_input_stream = null;
	private DataOutputStream _index_output_stream = null;
	private DataOutputStream _data_output_stream = null;
	private FileReader _reader = null;
	private byte _sig[] = new byte[8];
	private int _cacheLength = 0;
	private int _cacheType = 0;
	private long _countingOffset = 0;
	private Path _index_file = null;
	private Path _data_file = null;
	private long _imageCount = -1;

	private long _blockSize = 0;
	private short _replication = 0;

	/**
	 * 
	 * @param file_path The {@link Path} indicating where the image bundle is (or should be written to)
	 * @param conf {@link Configuration} that determines the {@link FileSystem} for the image bundle
	 */
	public HipiImageBundle(Path file_path, Configuration conf) {
		super(file_path, conf);
	}
	
	public HipiImageBundle(Path file_path, Configuration conf, short replication) {
		super(file_path, conf);
		_replication = replication;
	}
	
	public HipiImageBundle(Path file_path, Configuration conf, long blockSize) {
		super(file_path, conf);
		_blockSize = blockSize;
	}
	
	public HipiImageBundle(Path file_path, Configuration conf, short replication, long blockSize) {
		super(file_path, conf);
		_replication = replication;
		_blockSize = blockSize;
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
		// 8-byte of offsets (of the end position) starts from here until EOF
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void openForWrite() throws IOException {
		// Check if the instance is already in some read/write states
		if (_data_output_stream != null || _reader != null
				|| _index_output_stream != null || _index_input_stream != null) {
			throw new IOException("File " + _file_path.getName()
					+ " already opened for reading/writing");
		}

		// HipiImageBundle will create two files: an index file and a data file
		// by default, the Path of output_file is the Path of index file, and
		// data
		// file will simply append .dat suffix
		_index_file = _file_path;
		FileSystem fs = FileSystem.get(_conf);
		_index_output_stream = new DataOutputStream(fs.create(_index_file));
		_data_file = _file_path.suffix(".dat");
		if (_blockSize <= 0)
			_blockSize = fs.getDefaultBlockSize();
		if (_replication <= 0)
			_replication = fs.getDefaultReplication();
		_data_output_stream = new DataOutputStream(fs.create(_data_file, true, fs.getConf().getInt("io.file.buffer.size", 4096), _replication, _blockSize));
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

	/**
	 * 
	 * @return a {@link List} of image offsets
	 */
	public List<Long> getOffsets() {
		return getOffsets(0);
	}

	/**
	 * 
	 * @return The data file for the HipiImageBundle
	 * @throws IOException
	 */
	public FileStatus getDataFile() throws IOException {
		return FileSystem.get(_conf).getFileStatus(_data_file);
	}

	
	public List<Long> getOffsets(int maximumNumber) {
		ArrayList<Long> offsets = new ArrayList<Long>(maximumNumber);
		for (int i = 0; i < maximumNumber || maximumNumber == 0; i++) {
			try {
				offsets.add(_index_input_stream.readLong());
			} catch (IOException e) {
				break;
			}
		}
		return offsets;
	}


	@Override
	protected void openForRead() throws IOException {
		if (_data_output_stream != null || _reader != null
				|| _index_output_stream != null || _index_input_stream != null) {
			throw new IOException("File " + _file_path.getName()
					+ " already opened for reading/writing");
		}

		_index_file = _file_path;
		_index_input_stream = new DataInputStream(FileSystem.get(_conf).open(
				_index_file));

		readBundleHeader();

		_reader = new FileReader(FileSystem.get(_conf), _data_file, _conf, 0, 0);
	}

	/**
	 * Adds the image to the HipiImageBundle. This involves appending the image to the data file, and adding
	 * the image offset to the index file.
	 */
	@Override
	public void addImage(InputStream image_stream, ImageType type)
	throws IOException {
		byte data[] = readBytes(image_stream);
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
		_countingOffset += 8 + data.length;
		_index_output_stream.writeLong(_countingOffset);
	}

	private byte[] readBytes(InputStream stream) throws IOException {
		if (stream == null) return new byte[] {};
		byte[] buffer = new byte[1024];
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		boolean error = false;
		try {
			int numRead = 0;
			while ((numRead = stream.read(buffer)) > -1) {
				output.write(buffer, 0, numRead);
			}
		} catch (IOException e) {
			error = true; // this error should be thrown, even if there is an error closing stream
			throw e;
		} catch (RuntimeException e) {
			error = true; // this error should be thrown, even if there is an error closing stream
			throw e;
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				if (!error) throw e;
			}
		}
		output.flush();
		return output.toByteArray();
	}

	@Override
	public long getImageCount() {
		return _imageCount;
	}

	/**
	 * Implemented with {@link HipiImageBundle.FileReader#getCurrentKey()}
	 */
	@Override
	protected ImageHeader readHeader() throws IOException {
		return _reader.getCurrentKey();
	}

	/**
	 * Implemented with {@link HipiImageBundle.FileReader#getCurrentValue()}
	 */
	@Override
	protected FloatImage readImage() throws IOException {
		return _reader.getCurrentValue();
	}

	/**
	 * Implemented with {@link HipiImageBundle.FileReader#nextKeyValue()}
	 */
	@Override
	protected boolean prepareNext() {
		return _reader.nextKeyValue();
	}


	@Override
	public void close() throws IOException {

		if (_reader != null) {
			_reader.close();
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

	/**
	 * Appends a HipiImageBundle. This involves concatenating data files as well appending offsets to the index file.
	 * @param bundle HipiImageBundle to be appended
	 */
	/* Assumes that openForWrite has been called
	 */
	public void append(HipiImageBundle bundle) {
		try {
			bundle.open(FILE_MODE_READ, true);
			FileStatus data_file = bundle.getDataFile();
			List<Long> offsets = bundle.getOffsets();

			//concatenate data file
			FileSystem fs = FileSystem.get(_conf);
			DataInputStream data_input = new DataInputStream(fs.open(data_file.getPath()));
			int numRead = 0;
			byte[] data = new byte[1024*1024];
			while ((numRead = data_input.read(data)) > -1) {
				_data_output_stream.write(data, 0, numRead);
			}
			data_input.close();
			//write offsets in index file
			long last_offset = _countingOffset;
			for(int j = 0; j < offsets.size(); j++){
				_countingOffset = (long)(offsets.get(j)) + last_offset;
				_index_output_stream.writeLong(_countingOffset);
			}
			_data_output_stream.flush();
			_index_output_stream.flush();
			bundle.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
