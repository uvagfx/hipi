package hipi.imagebundle;

import hipi.image.HipiImage;
import hipi.image.HipiImageFactory;
import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.image.io.ImageEncoder;
import hipi.image.io.ImageDecoder;
import hipi.image.io.JpegCodec;
import hipi.image.io.PngCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Base class for all image bundles in HIPI. All subclasses must implement methods to open, read,
 * and close the image bundles.
 * 
 * This class can also be used to write an image bundle but image bundles have to be read and written
 * sequentially. Thus, in order to create an image bundle, you have to open a new file and then
 * write the entire contents. Once you have opened the file you cannot read anything from the file
 * until you have closed it.
 * 
 */
public abstract class AbstractImageBundle {

  public static final int FILE_MODE_READ = 1;
  public static final int FILE_MODE_WRITE = 2;

  private int fileMode = -1;

  protected Configuration conf;

  private boolean hasNext;
  private boolean prepared;
  private boolean readHeader;
  private HipiImage readImage;
  protected Path filePath;

  protected HipiImageFactory imageFactory;

  /**
   *
   * @param imageFactory The {@link HipiImageFactory} required to
   *        generate HipiImage objects (if this is equal to null the
   *        read image functions will not function)
   *
   * @param filePath The {@link Path} indicating where the image
   *        bundle is (or should be written to)
   *
   * @param conf {@link Configuration} that determines the {@link
   * FileSystem} for the image bundle
   */
  public AbstractImageBundle(HipiImageFactory imageFactory, Path filePath, Configuration conf) 
  {
    this.imageFactory = imageFactory;
    this.filePath = filePath;
    this.conf = conf;
  }

  /**
   * @param filePath The {@link Path} indicating where the image
   *        bundle is (or should be written to)
   *
   * @param conf {@link Configuration} that determines the {@link
   * FileSystem} for the image bundle
   */
  public AbstractImageBundle(Path filePath, Configuration conf) 
  {
    this(null, filePath, conf);
  }

  public final void open(int mode) throws IOException 
  {
    open(mode, false);
  }

  /**
   * Opens a file for either reading or writing. This method will
   * return an IOException if an open call has already happened.
   * 
   * @param mode determines whether the file will be read from or
   *        written to
   * @param overwrite if the file exists and this is a write
   *        operation, this parameter determines whether to delete the
   *        file first or throw an error
   * @throws IOException
   */
  public final void open(int mode, boolean overwrite) throws IOException {

    if (fileMode == -1 && mode == FILE_MODE_WRITE) {
      // Check to see whether the file exists
      if (FileSystem.get(conf).exists(filePath) && !overwrite) {
        throw new IOException("File " + filePath.getName() + " already exists");
      }
      fileMode = FILE_MODE_WRITE;
      openForWrite();
    } else if (fileMode == -1 && mode == FILE_MODE_READ) {
      fileMode = FILE_MODE_READ;
      openForRead();
    } else {
      throw new IOException("File " + filePath.getName() + " already opened");
    }
    prepared = false;
    readHeader = false;
    readImage = null;
  }

  /**
   * Method for opening a file for the purposes of writing. The
   * function {@link #open(int)} contains the necessary checks to
   * determine whether a file can be opened for writing.
   * 
   * @throws IOException
   */
  protected abstract void openForWrite() throws IOException;

  /**
   * Method for opening a file for the purposes of reading. The function {@link #open(int)} contains
   * the necessary checks to determine whether a file can be opened for reading.
   * 
   * @throws IOException
   */
  protected abstract void openForRead() throws IOException;

  /**
   * Add image to bundle.
   * 
   * @throws IOException
   */
  public abstract void addImage(HipiImageHeader imageHeader, InputStream inputStream) throws IOException;

  /**
   * Add image to bundle.
   * 
   * @throws IOException
   */
  public void addImage(InputStream inputStream, HipiImageFormat imageFormat) 
    throws IllegalArgumentException, IOException {
    ImageDecoder decoder = null;
    switch (imageFormat) {
      case JPEG:
	decoder = JpegCodec.getInstance();
	break;
      case PNG:
	decoder = PngCodec.getInstance();
	break;
      case PPM:
	throw new IllegalArgumentException("Not implemented.");
      case UNDEFINED:
      defult:
	throw new IllegalArgumentException("Unrecognized or unsupported image format.");
      }

    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    bufferedInputStream.mark(Integer.MAX_VALUE); // 100MB
    HipiImageHeader header = decoder.decodeHeader(bufferedInputStream);
    bufferedInputStream.reset();
    addImage(header, bufferedInputStream);
  }

  /**
   * Return the path to the index file
   * 
   * @return Path path to index file
   */
  public Path getPath() {
    return filePath;
  }

  /**
   * Reads the next image and stores it in a cache. Does not decode the image from the image bundle
   * 
   * @return denote if has next or not
   */
  protected abstract boolean prepareNext();

  /**
   * @return the decoded ImageHeader from the cache that has been prepared. Will not advance the
   *         bundle to the next image upon return
   * @throws IOException
   */
  protected abstract HipiImageHeader readHeader() throws IOException;

  /**
   * @return the decoded FloatImage from the cache that has been
   *         prepared. Will not advance the bundle to the next image
   *         upon return
   * 
   * @throws IOException
   */
  protected abstract HipiImage readImage() throws IOException;

  /**
   * Advances the image bundle to the next image
   * 
   * @return ImageHeader of the next image
   * @throws IOException
   */
  public final HipiImageHeader next() throws IOException {
    if (!prepared) {
      hasNext = prepareNext();
    }
    prepared = false;
    readImage = null;
    if (hasNext) {
      readHeader = true;
      return readHeader();
    } else {
      readHeader = false;
      return null;
    }
  }

  /**
   * 
   * @return the HipiImage of the image at the current position in the
   * image bundle
   * @throws IOException
   */
  public final HipiImage getCurrentImage() throws IOException {
    if (readImage == null && readHeader) {
      readImage = readImage();
    }
    return readImage;
  }

  /**
   * @return a boolean indicating whether there are more images left to read from this bundle.
   */
  public boolean hasNext() {
    if (!prepared) {
      hasNext = prepareNext();
      prepared = true;
    }
    return hasNext;
  }

  /**
   * Closes the underlying stream for this bundle. In some implementations, no data is written to
   * the output stream unless this function is called.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;
}
