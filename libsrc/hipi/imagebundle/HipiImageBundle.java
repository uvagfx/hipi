package hipi.imagebundle;

import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageFormat;
import hipi.image.HipiImage;
//import hipi.image.RasterImage;
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
 * A HipiImageBundle (HIB) is the primary representation for a
 * collection of images on the Hadoop Distributed File System (HDFS)
 * used by HIPI.
 *
 * HIBs are designed to take advantage of the fact that Hadoop
 * MapReduce is optimized to support efficient processing of large
 * flat files.
 *
 * This class provides basic methods for writing, reading, and
 * concatenating HIBs.
 * 
 * A single HIB is actually comprised of two files stored on the file
 * system: an index file and a data file. The index file contains a
 * list of byte offsets to the end of each image record (header
 * metadata + image pixel data) in the data file. The data file is
 * composed of a contiguous sequence of image records.
 *
 * @see <a href="http://hipi.cs.virginia.edu/">HIPI Project Homepage</a>
 */

public class HipiImageBundle {  // <S extends RasterImage<T>> extends AbstractImageBundle {

  /**
   * This FileReader enables reading individual images from a {@link
   * hipi.imagebundle.HipiImageBundle} and delivers them in the
   * specified image type. This class is used by the {@link
   * hipi.imagebundle.mapreduce.HipiImageBundleInputFormat} and {@link
   * hipi.imagebundle.mapreduce.HipiImageBundleRecordReader}.
   *
   */
  public static class FileReader<ImageType extends HipiImage> {

    // Input stream connected to HIB data file
    private DataInputStream dataInputStream = null;

    // Current position and start/end offsets in input stream
    private long currentOffset = 0;
    private long startOffset = 0;
    private long endOffset = 0;

    // Each image record in the data file begins with a 12 byte
    // "signature" that indicates length of header, length of image
    // data, and image storage format in that order
    private byte sig[] = new byte[12];

    // Current image, accessed with calls to getCurrentKey and
    // getCurrentValue
    private ImageFormat imageFormat = UNDEFINED;
    private byte[] imageBytes = null;
    private ImageHeader imageHeader = null;
    private S image = null;

    /**
     * Creates a FileReader to read records (image headers / image
     * bodies) from a contiguous segment (file split) of a HIB data
     * file. The segment is specified by a start and end byte offset.
     * 
     * @param fs The {@link FileSystem} where the HIB data file resides
     * @param path The {@link Path} to the HIB data file
     * @param conf The {@link Configuration} for the associted MapReduce job
     * @param start The byte offset to beginning of segment
     * @param end The byte offset to end of segment
     * @throws IOException
     */
    public FileReader(FileSystem fs, Path path, Configuration conf, long start, long end) throws IOException {

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
     * Reads the next image header and image body into memory. To
     * obtain the corresponding {@link ImageHeader} and {@link
     * RasterImage} objects, call {@link #getCurrentKey()} and {@link
     * #getCurrentValue()} respectively.
     * 
     * @return True if the next image was successfully read. False if
     * there are no more images or if an error occurs. Check stderr
     * logs for errors.
     */
    public boolean nextKeyValue() {
      
      try {

	// Reset state of current key/value
	imageFormat = UNDEFINED;
	imageBytes = null;
	imageHeader = null;
	image = null;
	    
	// A value of endOffset = 0 indicates "read to the end of
	// file", otherwise check segment boundary
	if (endOffset > 0 && currentOffset > endOffset) {
	  // Already past end of file segment
	  return false;
	}

	// Attempt to read "signature" that contains length of image
	// header, length of image data segment, and storage format
	dataInputStream.readFully(sig);

	// Parse and validate image header length
	int imageHeaderLength = ((sig[0] & 0xff) << 24) | ((sig[1] & 0xff) << 16) | ((sig[2] & 0xff) << 8) | (sig[3] & 0xff);
	if (imageHeaderLength <= 0) {
	  // Negative or zero file length, report corrupted HIB
	  throw new IOException("Found image header length <= 0 in HIB at offset: " + countingOffset);
	}

	// Parse and validate image length
	int imageLength = ((sig[4] & 0xff) << 24) | ((sig[5] & 0xff) << 16) | ((sig[6] & 0xff) << 8) | (sig[7] & 0xff);
	if (imageLength <= 0) {
	  // Negative or zero file length, report corrupted HIB
	  throw new IOException("Found image data segment length <= 0 in HIB at offset: " + countingOffset);
	}

	// Parse and validate image format
	int imageFormatInt = ((sig[8] & 0xff) << 24) | ((sig[9] & 0xff) << 16) | ((sig[10] & 0xff) << 8) | (sig[11] & 0xff);
	try {
	  imageFormat = ImageFormat.fromInteger(imageFormatInat);
	} catch (IllegalArgumentException e) {
	  throw new IOException("Found invalid image storage format in HIB at offset: " + currentOffset);
	}
	if (imageFormat == UNDEFINED) {
	  throw new IOException("Found UNDEFINED image storage format in HIB at offset: " + currentOffset);
	}

	// Allocate byte array to hold image header data
	byte[] imageHeaderBytes = new byte[imageHeaderLength];

	// Allocate byte array to hold (possibly compressed and
	// encoded) image data
	imageBytes = new byte[imageLength];

	//
	// TODO: What happens if either of these calls fails, throwing
	// an exception? The stream position will become out of sync
	// with currentOffset...
	//
	dataInputStream.readFully(imageHeaderBytes);
	dataInputStream.readFully(imageBytes);

	// Advance byte offset by length of 12-byte signature plus
	// image header length plus image pixel data length
	currentOffset += 12 + imageHeaderLength + imageLength;

	// Attempt to decode image header
	DataInputStream dis = new DataInputStream(new ByteArrayInputStream(imageHeaderBytes));
	imageHeader = new ImageHeader(dis);
	if (!imageHeader) {
	  throw new IOException("Failed to read and decode image header.");
	}

	//
	// TODO: Perform cull step here? Continue advancing
	// dataInputStream until a valid image record is found. Would
	// be ever better to perform cull before imageBytes are read.
	//

	ImageDecoder decoder = CodecManager.getDecoder(imageFormat);
	if (decoder == null) {
	  throw new IOException("Unsupported storage format in image record ending at byte offset: " + currentOffset);
	}

	image = new ImageType(imageHeader);
	if (!decoder.decodeImage(new ByteArrayInputStream(imageBytes), image)) {
	  throw new IOException("Failed to decode image data in image record ending at byte offset: " + currentOffset);
	}

	return true;

      } catch (IOException e) {
	System.err.println(String.format("IO exception [%s] while decoding HIB image record at byte offset [%d]",
					 e.getMessage(), currentOffset));
	imageFormat = UNDEFINED;
	imageBytes = null;
	imageHeader = null;
	image = null;
	return false;
      } catch (EOFException e) {
	if (endOffset > 0) {
	  System.err.println(String.format("EOF exception [%s] while reading HIB file at byte offset [%d] with end byte offset [%d]", 
					   e.getMessage(), currentOffset, endOffset));
	}
	imageFormat = UNDEFINED;
	imageBytes = null;
	imageHeader = null;
	image = null;
	return false;
      }
    }

    /**
     * @return Byte array containing raw image data.
     */
    public byte[] getImageBytes() {
      return imageBytes;
    }

    /**
     * @return Storage format of raw image bytes.
     */
    public ImageFormat getImageStorageFormat() {
      return imageFormat;
    }

    /**
     * @return Header for the current image, as retrieved by {@link
     * #nextKeyValue()}
     */
    public ImageHeader getCurrentKey() {
      return imageHeader;
    }

    /**
     * @return Current decoded image, as retrieved by {@link
     * #nextKeyValue()}
     */
    public S getCurrentValue() {
      return image;
    }

  } // public static class FileReader

  private DataInputStream indexInputStream = null;
  private DataOutputStream indexOutputStream = null;
  private DataOutputStream dataOutputStream = null;
  private FileReader reader = null;
  private byte sig[] = new byte[12];
  private int imageHeaderLength = 0;
  private int imageLength = 0;
  private ImageFormat imageFormat = UNDEFINED;
  private long currentOffset = 0;
  private Path indexFilePath = null;
  private Path dataFilePath = null;
  private long imageCount = 0;

  private long blockSize = 0;
  private short replication = 0;

  public HipiImageBundle(Path filePath, Configuration conf) {
    super(filePath, conf);
  }

  public HipiImageBundle(Path filePath, Configuration conf, short replication) {
    super(filePath, conf);
    this.replication = replication;
  }

  public HipiImageBundle(Path filePath, Configuration conf, long blockSize) {
    super(filePath, conf);
    this.blockSize = blockSize;
  }

  public HipiImageBundle(Path filePath, Configuration conf, short replication, long blockSize) {
    super(filePath, conf);
    replication = replication;
    blockSize = blockSize;
  }

  /**
   * HIB index file header structure:
   * BOF
   * 4 bytes (int): magic signature (0x81911618) "HIPIIbIH"
   * 2 bytes (short int): length of data file name
   * var bytes: data file path name
   * 8 bytes: image count (-1 => unknown)
   * 16 bytes: reserved for future use
   * 4 bytes: number of bytes to skip to reach start of offset list
   * [8 byte]*: offsets
   * EOF
   */
  private void writeBundleHeader() throws IOException {
    indexOutputStream.writeInt(0x81911b18);
    String dataFileName = dataFilePath.getName();
    // write out filename in UTF-8 encoding
    byte[] dataFileNameBytes = dataFileName.getBytes("UTF-8");
    indexOutputStream.writeShort(dataFilePathNameBytes.length);
    indexOutputStream.write(dataFileNameBytes);
    // write out image count (-1 => unknown)
    indexOutputStream.writeLong(-1);
    // write out reserved field (0)
    indexOutputStream.writeLong(0);
    indexOutputStream.writeLong(0);
    // write skip value of 0 bytes to reach start of offsets (this
    // convention allows future versions to insert variable length
    // data in hib header)
    indexOutputStream.writeInt(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override 
  protected void openForWrite() throws IOException {

    // Check if this object is already in an opened state
    if (indexOutputStream != null || dataOutputStream != null) {
      throw new IOException("File " + filePath.getName() + " already opened for WRITING");
    }

    if (indexInputStream != null || reader != null) {
      throw new IOException("File " + filePath.getName() + " already opened for READING");
    }

    // HipiImageBundle will create two files: an index file (provided
    // by filePath from AbstractImageBundle) and a data file which is
    // name of index file with .dat suffix
    indexFilePath = filePath;
    FileSystem fs = FileSystem.get(conf);
    indexOutputStream = new DataOutputStream(fs.create(indexFilePath));
    dataFilePath = filePath.suffix(".dat");

    if (blockSize <= 0) {
      blockSize = fs.getDefaultBlockSize(filePath);
    }

    if (replication <= 0) {
      replication = fs.getDefaultReplication(filePath);
    }

    dataOutputStream = new DataOutputStream(fs.create(dataFilePath, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize));

    currentOffset = 0;
    
    writeBundleHeader();
  }

  private void readBundleHeader() throws IOException {

    // Verify signature
    int sig = indexInputStream.readInt();
    if (sig != 0x81911b18) {
      throw new IOException("Corrupted HIB header: signature mismatch.");
    }

    // Read and decode name of data file
    short dataFileNameLength = indexInputStream.readShort();
    byte[] dataFileNameBytes = new byte[dataFileNameLength];
    indexInputStream.readFully(dataFileNameBytes);
    String dataFileName = new String(dataFileNameBytes, "UTF-8");
    dataFilePath = new Path(indexFilePath.getParent(), dataFileName);
    System.out.println("Found data file path: " + dataFilePath);

    // Read image count
    imageCount = indexInputStream.readLong();

    // Use readLong to skip reserved fields instead of skip because
    // skip doesn't guarantee success. If readLong reaches EOF will
    // throw exception.
    indexInputStream.readLong();
    indexInputStream.readLong();

    int skipOver = indexInputStream.readInt();
    while (skipOver > 0) {
      long skipped = indexInputStream.skip(skipOver);
      if (skipped <= 0) {
        break;
      }
      skipOver -= skipped;
    }
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
  public FileStatus getDataFileStatus() throws IOException {
    return FileSystem.get(conf).getFileStatus(dataFilePath);
  }

  public List<Long> getOffsets(int maximumNumber) {
    ArrayList<Long> offsets = new ArrayList<Long>(maximumNumber);
    for (int i = 0; i < maximumNumber || maximumNumber == 0; i++) {
      try {
        offsets.add(indexInputStream.readLong());
      } catch (IOException e) {
	System.err.println("IOException while reading offsets from HIB index file at position: " + i);
        break;
      }
    }
    return offsets;
  }

  @Override
  protected void openForRead() throws IOException {

    // Check if this object is already in an opened state
    if (indexOutputStream != null || dataOutputStream != null) {
      throw new IOException("File " + filePath.getName() + " already opened for WRITING");
    }

    if (indexInputStream != null || reader != null) {
      throw new IOException("File " + filePath.getName() + " already opened for READING");
    }
    
    indexFilePath = filePath;
    indexInputStream = new DataInputStream(FileSystem.get(conf).open(indexFilePath));

    readBundleHeader();

    reader = new FileReader(FileSystem.get(conf), dataFilePath, conf, 0, 0);
  }

  /**
   * Adds the image to the HipiImageBundle. This involves appending the image to the data file, and
   * adding the image offset to the index file.
   */
  @Override
  public void addImage(ImageFormat imageFormat, ImageHeader imageHeader, InputStream imageStream) throws IOException {

    // TODO: Verify that HIB is in proper state for this operation.

    // Serialize imageHeader into byte[]
    ByteArrayOutputStream imageHeaderStream = new ByteArrayOutputStream(1024);
    imageHeader.write(new DataOutputStream(imageHeaderStream));
    byte imageHeaderBytes[] = imageHeaderStream.toByteArray();
    int imageHeaderLength = imageHeaderBytes.length;

    byte imageBytes[] = inputStreamToBytes(imageStream);
    int imageLength = imageBytes.length;

    int imageFormatInt = imageFormat.toValue();

    _cacheLength = data.length;
    _cacheType = type.toValue();

    sig[ 0] = (byte)((imageHeaderLength >> 24)       );
    sig[ 1] = (byte)((imageHeaderLength >> 16) & 0xff);
    sig[ 2] = (byte)((imageHeaderLength >>  8) & 0xff);
    sig[ 3] = (byte)((imageHeaderLength      ) & 0xff);

    sig[ 4] = (byte)((imageLength >> 24)       );
    sig[ 5] = (byte)((imageLength >> 16) & 0xff);
    sig[ 6] = (byte)((imageLength >>  8) & 0xff);
    sig[ 7] = (byte)((imageLength      ) & 0xff);

    sig[ 8] = (byte)((imageFormatInt >> 24)       );
    sig[ 9] = (byte)((imageFormatInt >> 16) & 0xff);
    sig[10] = (byte)((imageFormatInt >>  8) & 0xff);
    sig[11] = (byte)((imageFormatInt      ) & 0xff);

    dataOutputStream.write(sig);
    dataOutputStream.write(imageHeaderBytes);
    dataOutputStream.write(imageBytes);

    currentOffset += 12 + imageHeaderLength + imageLength;
    indexOutputStream.writeLong(currentOffset);

    imageCount++;
  }

  private byte[] inputStreamToBytes(InputStream stream) throws IOException {
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
    return imageCount;
  }

  /**
   * Implemented with {@link HipiImageBundle.FileReader#getCurrentKey()}
   */
  @Override
  protected ImageHeader readHeader() throws IOException {
    return reader.getCurrentKey();
  }

  /**
   * Implemented with {@link HipiImageBundle.FileReader#getCurrentValue()}
   */
  @Override
  protected T readImage() throws IOException {
    return reader.getCurrentValue();
  }

  /**
   * Implemented with {@link HipiImageBundle.FileReader#nextKeyValue()}
   */
  @Override
  protected boolean prepareNext() {
    return reader.nextKeyValue();
  }

  @Override
  public void close() throws IOException {

    if (reader != null) {
      reader.close();
    }

    if (indexInputStream != null) {
      indexInputStream.close();
    }

    if (dataOutputStream != null) {
      dataOutputStream.close();
    }

    if (indexOutputStream != null) {
      indexOutputStream.close();
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
    // TODO: Check that bundle is in a state that supports this operation
    try {
      bundle.open(FILE_MODE_READ, true);
      FileStatus dataFileStatus = bundle.getDataFileStatus();
      List<Long> offsets = bundle.getOffsets();

      // Concatenate data file
      FileSystem fs = FileSystem.get(conf);
      DataInputStream dataInputStream = new DataInputStream(fs.open(dataFileStatus.getPath()));
      int bytesRead = 0;
      byte[] data = new byte[1024 * 1024]; // Transfer in 1MB blocks
      while ((bytesRead = dataInputStream.read(data)) > -1) {
        dataOutputStream.write(data, 0, numRead);
      }
      dataInputStream.close();

      // Concatenate index file
      long lastOffset = currentOffset;
      for (int j = 0; j < offsets.size(); j++) {
        currentOffset = (long) (offsets.get(j)) + lastOffset;
        indexOutputStream.writeLong(currentOffset);
      }

      // Update image count
      imageCount += offsets.size();

      // Clean up
      dataOutputStream.flush();
      indexOutputStream.flush();
      bundle.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
