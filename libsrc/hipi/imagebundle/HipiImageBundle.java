package hipi.imagebundle;

import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageFormat;
import hipi.image.RasterImage;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A HipiImageBundle (HIB) is HIPI's mechanism for storing a
 * collection of images on the Hadoop Distributed File System
 * (HDFS). It takes advantage of the fact that Hadoop MapReduce is
 * designed to support efficient processing of large flat files. This
 * class provides methods for writing, reading, and appending
 * HIBs. HIBs consist of two files: an index file and a data file. The
 * index file contains a list of byte offsets to the end of each image
 * in the data file. Note that this class is templated with the T
 * class. T is the type of object returned by the image accessor
 * methods {@link HipiImageBundle.FileReader#getCurrentValue()} and
 * {@link HipiImageBundle#readImage()}.
 * 
 * @see <a href="http://hipi.cs.virginia.edu/">HIPI Project Homepage</a>
 */

public class HipiImageBundle<S extends RasterImage<T>> extends AbstractImageBundle {

  /**
   * This FileReader class provides an interface for reading individual images from a
   * {@link hipi.imagebundle.HipiImageBundle}. This class is used by the 
   * {@link hipi.imagebundle.mapreduce.ImageBundleInputFormat} and
   * {@link hipi.imagebundle.mapreduce.ImageBundleRecordReader}.
   *
   */
  public static class FileReader {

    // Input stream
    private DataInputStream dataInputStream = null;

    // Current position and start/end offsets in input stream
    private long currentOffset = 0;
    private long startOffset = 0;
    private long endOffset = 0;

    // Variables used to read and parse each image record
    private byte sig[] = new byte[8];
    private ImageFormat imageFormat = UNDEFINED;

    // Current image, accessed with calls to getCurrentKey and
    // getCurrentValue
    private byte[] imageBytes = null;
    private ImageHeader imageHeader = null;
    private S image = null;

    /**
     * Creates a FileReader to read records (image headers / image
     * bodies) from a segment of a HIB data file. The segment is
     * specified with a start and end byte offset.
     * 
     * @param fs The {@link FileSystem} where the HIB data file resides
     * @param path The {@link Path} to the HIB data file
     * @param conf The {@link Configuration} for the associted MapReduce job
     * @param start The byte offset to beginning of segment
     * @param end The byte offset to end of segment
     * @throws IOException
     */
    public FileReader(FileSystem fs, Path path, Configuration conf, 
        long start, long end) throws IOException  {

      // Create input stream for HIB data file
      dataInputStream = new DataInputStream(fs.open(path));

      // Advance input stream to requested start byte offset. This may
      // take several calls to the DataInputStream.skip() method.
      startOffset = start;
      while (start > 0) {
	long skipped = dataInputStream.skip(start);
	if (skipped <= 0) {
	  break;
	}
	start -= skipped;
      }

      // Store current byte offset along with end byte offset
      currentOffset = startOffset;
      endOffset = end;
    }

    /**
     * Returns current amount of progress reading file.
     * 
     * @return Measure of progress from 0.0 (no progress) to 1.0
     * (finished).
     */
    public float getProgress()  {
      float progress = (endOffset - startOffset + 1) > 0 ? (float) (currentOffset - startOffset) / (float) (endOffset - startOffset + 1) : 0;
      // Clamp to handle rounding errors
      if (progress > 1.0) {
	return 1.0;
      } else if (progress < 0.0) {
	return 0.0;
      }
      return progress;
    }

    /**
     * Closes any open objects used to read the HIB data file (e.g.,
     * DataInputStream).
     *
     * @throws IOException
     */
    public void close() throws IOException {
      if (dataInputStream != null) {
	dataInputStream.close();
      }
    }

    /**
     * Reads the next image header and image body into memory. Note
     * that the image header is decoded in this method, but the image
     * pixel data is not. To obtain the corresponding {@link
     * ImageHeader} and ImageType (template parameter) object call
     * {@link #getCurrentKey()} and {@link #getCurrentValue()}
     * respectively.
     * 
     * @return True if the reader could get the next image from the
     *         HIB. False if there are no more images to read or if an
     *         error occurs.
     */
    public boolean nextKeyValue() {
      
      try {

	imageType = UNDEFINED;
	imageBytes = null;
	imageHeader = null;
	image = null;
	    
	// A value of endOffset = 0 indicates "read to the end of
	// file", otherwise check segment boundary
	if (endOffset > 0 && currentOffset > endOffset) {
	  // Already past end of file segment
	  return false;
	}

	// Attempt to read eight-byte "signature" that contains length of image and format
	dataInputStream.readFully(sig);

	// Parse and validate image length
	int imageLength = ((sig[0] & 0xff) << 24) | ((sig[1] & 0xff) << 16) | ((sig[2] & 0xff) << 8) | (sig[3] & 0xff);
	if (imageLength <= 0) {
	  // Negative or zero file length, report corrupted HIB
	  throw new IOException("Found image record length <= 0 in HIB at offset: " + countingOffset);
	}

	// Parse and validate image format
	int imageFormatInt = ((sig[4] & 0xff) << 24) | ((sig[5] & 0xff) << 16) | ((sig[6] & 0xff) << 8) | (sig[7] & 0xff);
	try {
	  imageFormat = ImageFormat.fromInteger(recordTypeInt);
	} catch (IllegalArgumentException e) {
	  throw new IOException("Found invalid image format in HIB at offset: " + currentOffset);
	}
	if (imageFormat == UNDEFINED) {
	  throw new IOException("Found UNDEFINED image format in HIB at offset: " + currentOffset);
	}

	// Allocate byte array to hold (possibly compressed and encoded) image data
	imageBytes = new byte[imageLength];

	// Read image bytes from input stream
	dataInputStream.readFully(imageBytes);

	// Advance byte offset by image length plus 8 bytes for signature
	currentOffset += imageLength + 8;

	return true;

      } catch (IOException e) {
	System.err.println(String.format("Unexpected IO exception [%s] while reading HIB file at byte offset [%d]", 
					 e.getMessage(), currentOffset));
	imageLength = 0;
	imageType = UNDEFINED;
	imageBytes = null;
	return false;
      } catch (EOFException e) {
	// Reached end of file
	if (endOffset > 0) {
	  System.err.println(String.format("Unexpected EOF exception [%s] while reading HIB file at byte offset [%d] with end byte offset [%d]", 
					   e.getMessage(), currentOffset, endOffset));
	}
	imageLength = 0;
	imageType = UNDEFINED;
	imageBytes = null;
	return false;
      }
    }

    /**
     * @return Raw byte array containing the image as stored in the
     *         HipiImageBundle.
     * 
     * @throws IOException
     */
    public byte[] getRawImageBytes() throws IOException {
      return imageBytes;
    }

    /**
     * @return ImageHeader of the current image, as retrieved by {@link #nextKeyValue()}
     * @throws IOException
     */
    public ImageHeader getCurrentKey() throws IOException {
      // Check if it's already been decoded
      if (imageHeader != null) {
	return imageHeader;
      }
      
      if (imageLength > 0) {
	ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
	if (decoder == null) {
	  System.out.println("decoder is null");
	  return null;
	}
	ByteArrayInputStream _byte_array_input_stream = new ByteArrayInputStream(_byte_array_data);
	try {
	  _header = decoder.decodeImageHeader(_byte_array_input_stream);
	} catch (Exception e) {
	  e.printStackTrace();
	  _header = null;
	}
	return _header;
      }
      
      System.out.println("final case - null");
      return null;
    }

    /**
     * 
     * @return Decoded image as a {@link FloatImage}, as retrieved by {@link #nextKeyValue()}
     * @throws IOException
     */
    public FloatImage getCurrentValue() throws IOException {
      if (_image != null) {
	return _image;
      }

      if (_cacheLength > 0) {
	ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
	if (decoder == null) {
	  return null;
	}
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

  } // public static class FileReader

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

  /**
   * the index file header designed like this:
   * 4-byte magic signature (0x81911618) for "HIPIIbIH"
   * 2-byte (short int) to denote the length of data file name
   * variable bytes of data file name
   * 8-byte of image count (not mandated)
   * 16-byte of reserved field
   * 4-byte points to how much to skip in order to reach the start of
   * offsets (default 0)
   * 8-byte of offsets (of the end position) starts from here until EOF
   */
  private void writeBundleHeader() throws IOException {
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
    if (_data_output_stream != null || _reader != null || _index_output_stream != null
        || _index_input_stream != null) {
      throw new IOException("File " + _file_path.getName() + " already opened for reading/writing");
    }

    // HipiImageBundle will create two files: an index file and a data file
    // by default, the Path of output_file is the Path of index file, and
    // data file will simply append .dat suffix
    _index_file = _file_path;
    FileSystem fs = FileSystem.get(_conf);
    _index_output_stream = new DataOutputStream(fs.create(_index_file));
    _data_file = _file_path.suffix(".dat");
    if (_blockSize <= 0) {
      _blockSize = fs.getDefaultBlockSize(_file_path);
    }
    if (_replication <= 0) {
      _replication = fs.getDefaultReplication(_file_path);
    }
    _data_output_stream =
        new DataOutputStream(fs.create(_data_file, true,
            fs.getConf().getInt("io.file.buffer.size", 4096), _replication, _blockSize));
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
      if (skipped <= 0) {
        break;
      }
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
    if (_data_output_stream != null || _reader != null || _index_output_stream != null
        || _index_input_stream != null) {
      throw new IOException("File " + _file_path.getName() + " already opened for reading/writing");
    }
    
    _index_file = _file_path;
    _index_input_stream = new DataInputStream(FileSystem.get(_conf).open(_index_file));

    readBundleHeader();

    _reader = new FileReader(FileSystem.get(_conf), _data_file, _conf, 0, 0);
  }

  /**
   * Adds the image to the HipiImageBundle. This involves appending the image to the data file, and
   * adding the image offset to the index file.
   */
  @Override
  public void addImage(InputStream image_stream, ImageType type) throws IOException {
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
    if (stream == null) {
      return new byte[] {};
    }
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    boolean error = false;
    try {
      int numRead = 0;
      while ((numRead = stream.read(buffer)) > -1) {
        output.write(buffer, 0, numRead);
      }
    } catch (IOException ioe) {
      error = true; // this error should be thrown, even if there is an error closing stream
      throw ioe;
    } catch (RuntimeException re) {
      error = true; // this error should be thrown, even if there is an error closing stream
      throw re;
    } finally {
      try {
        stream.close();
      } catch (IOException ioe) {
        if (!error) {
          throw ioe;
        }
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
  protected T readImage() throws IOException {
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
   * Appends a HipiImageBundle. This involves concatenating data files as well appending offsets to
   * the index file.
   * 
   * @param bundle HipiImageBundle to be appended
   */
  /*
   * Assumes that openForWrite has been called
   */
  public void append(HipiImageBundle bundle) {
    try {
      bundle.open(FILE_MODE_READ, true);
      FileStatus data_file = bundle.getDataFile();
      List<Long> offsets = bundle.getOffsets();

      // concatenate data file
      FileSystem fs = FileSystem.get(_conf);
      DataInputStream data_input = new DataInputStream(fs.open(data_file.getPath()));
      int numRead = 0;
      byte[] data = new byte[1024 * 1024];
      while ((numRead = data_input.read(data)) > -1) {
        _data_output_stream.write(data, 0, numRead);
      }
      data_input.close();
      // write offsets in index file
      long last_offset = _countingOffset;
      for (int j = 0; j < offsets.size(); j++) {
        _countingOffset = (long) (offsets.get(j)) + last_offset;
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
