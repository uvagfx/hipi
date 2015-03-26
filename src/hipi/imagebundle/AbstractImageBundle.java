package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Base class for all image bundles in HIPI. All subclasses must implement
 * methods to open, read, and close the image bundles.
 * 
 * This class can also be used to write an image bundle but image bundles have
 * to read and written sequentially. Thus, in order to create an image bundle,
 * you have to open a new file and then write the entire contents. Once you have
 * opened the file you cannot read anything from the file until you have closed
 * it.
 * 
 */
public abstract class AbstractImageBundle {

	public static final int FILE_MODE_READ = 1;
	public static final int FILE_MODE_WRITE = 2;

	private int _fileMode = -1;

	protected Configuration _conf;

	private boolean _hasNext;
	private boolean _prepared;
	private boolean _readHeader;
	private FloatImage _readImage;
	protected Path _file_path;

	/**
	 * 
	 * @param file_path The {@link Path} indicating where the image bundle is (or should be written to)
	 * @param conf {@link Configuration} that determines the {@link FileSystem} for the image bundle
	 */
	public AbstractImageBundle(Path file_path, Configuration conf) {
		_file_path = file_path;
		_conf = conf;
	}

	public final void open(int mode) throws IOException {
		open(mode, false);
	}

	/**
	 * Opens a file for either reading or writing. This method will return an
	 * IOException if an open call has already happened.
	 * 
	 * @param mode
	 *            determines whether the file will be read from or written to
	 * @param overwrite
	 *            if the file exists and this is a write operation, this
	 *            parameter determines whether to delete the file first or throw
	 *            an error
	 * @throws IOException
	 */
	public final void open(int mode, boolean overwrite) throws IOException {

		if (_fileMode == -1 && mode == FILE_MODE_WRITE) {			
			// Check to see whether the file exists
			if (FileSystem.get(_conf).exists(_file_path) && !overwrite) {
				throw new IOException("File " + _file_path.getName()
						+ " already exists");
			}
			_fileMode = FILE_MODE_WRITE;
			openForWrite();
		} else if (_fileMode == -1 && mode == FILE_MODE_READ) {
			_fileMode = FILE_MODE_READ;
			openForRead();
		} else {
			throw new IOException("File " + _file_path.getName()
					+ " already opened for reading/writing");
		}
		_prepared = false;
		_readHeader = false;
		_readImage = null;
	}

	/**
	 * Method for opening a file for the purposes of writing. The function
	 * {@link #open(int)} contains the necessary
	 * checks to determine whether a file can be opened for writing.
	 * 
	 * @throws IOException
	 */
	protected abstract void openForWrite() throws IOException;

	/**
	 * Method for opening a file for the purposes of reading. The function
	 * {@link #open(int)} contains the necessary
	 * checks to determine whether a file can be opened for reading.
	 * 
	 * @throws IOException
	 */
	protected abstract void openForRead() throws IOException;

	/**
	 * Add an image to this bundle. Some implementations may not actually write
	 * the data to the file system until after close has been called.
	 * 
	 * @throws IOException
	 */
	public final void addImage(FloatImage image) throws IOException {
		addImage(image, JPEGImageUtil.getInstance());
	}
	
	public final void addImage(FloatImage image, ImageEncoder encoder) throws IOException {
		addImage(image, encoder, encoder.createSimpleHeader(image));
	}
	
	public final void addImage(FloatImage image, ImageEncoder encoder, ImageHeader header) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		encoder.encodeImage(image, header, baos);

		addImage(new ByteArrayInputStream(baos.toByteArray()), header.getImageType());
	}
	
	public abstract void addImage(InputStream image_stream, ImageType type) throws IOException;

	/**
	 * Get the number of images contained in this bundle
	 */
	public abstract long getImageCount();

	/**
	 * Return the path to the index file
	 * 
	 * @return Path path to index file
	 */
	public Path getPath(){
		return _file_path;
	}
	
	/**
	 * Reads the next image and stores it in a cache. Does not decode the image from the image bundle
	 * @return denote if has next or not
	 */
	protected abstract boolean prepareNext();
	
	/**
	 * @return the decoded ImageHeader from the cache that has been prepared. Will not advance the 
	 * bundle to the next image upon return
	 * @throws IOException
	 */
	protected abstract ImageHeader readHeader() throws IOException;
	
	/**
	 * @return the decoded FloatImage from the cache that has been prepared. Will not advance the 
	 * bundle to the next image upon return
	 * 
	 * @throws IOException
	 */
	protected abstract FloatImage readImage() throws IOException;

	/**
	 * Advances the image bundle to the next image
	 * @return ImageHeader of the next image
	 * @throws IOException
	 */
	public final ImageHeader next() throws IOException {
		if (!_prepared)
			_hasNext = prepareNext();
		_prepared = false;
		_readImage = null;
		if (_hasNext) {
			_readHeader = true;
			return readHeader();
		} else {
			_readHeader = false;
			return null;
		}
	}

	/**
	 * 
	 * @return the FloatImage of the image at the current position in the image bundle
	 * @throws IOException
	 */
	public final FloatImage getCurrentImage() throws IOException {
		if (_readImage == null && _readHeader) {
			_readImage = readImage();
		}
		return _readImage;
	}

	/**
	 * @return a boolean indicating whether there are more images left to read
	 * from this bundle.
	 */
	public boolean hasNext() {
		if (!_prepared) {
			_hasNext = prepareNext();
			_prepared = true;
		}
		return _hasNext;
	}

	/**
	 * Closes the underlying stream for this bundle. In some implementations, no
	 * data is written to the output stream unless this function is called.
	 * 
	 * @throws IOException
	 */
	public abstract void close() throws IOException;
}
