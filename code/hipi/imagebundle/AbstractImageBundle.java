package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * @author seanarietta
 * 
 */
public abstract class AbstractImageBundle {

	private static final Log LOG = LogFactory.getLog(AbstractImageBundle.class
			.getName());

	public static final int FILE_MODE_READ = 1;
	public static final int FILE_MODE_WRITE = 2;

	private int _fileMode = -1;

	protected Configuration _conf;

	private boolean _readHeader;
	private boolean _readImage;

	public AbstractImageBundle(Configuration conf) {
		_conf = conf;
		_readHeader = false;
		_readImage = false;
	}

	public final void open(Path file_path, int mode) throws IOException {
		open(file_path, mode, false);
	}

	/**
	 * Opens a file for either reading or writing. This method will return an
	 * IOException if an open call has already happened.
	 * 
	 * @param file_path
	 *            the file that will be used for reading/writing
	 * @param mode
	 *            determines whether the file will be read from or written to
	 * @param overwrite
	 *            if the file exists and this is a write operation, this
	 *            parameter determines whether to delete the file first or throw
	 *            an error
	 * @throws IOException
	 */
	public final void open(Path file_path, int mode, boolean overwrite)
			throws IOException {
		LOG.info("Attempting to access file " + file_path.getName()
				+ " with mode " + mode);

		if (_fileMode == -1 && mode == FILE_MODE_WRITE) {
			LOG.info("Attempting to open file " + file_path.getName()
					+ " for writing (overwrite: " + overwrite + ")");
			// Check to see whether the file exists
			if (FileSystem.get(_conf).exists(file_path) && !overwrite) {
				throw new IOException("File " + file_path.getName()
						+ " already exists");
			}
			_fileMode = FILE_MODE_WRITE;
			openForWrite(file_path);
		} else if (_fileMode == -1 && mode == FILE_MODE_WRITE) {
			LOG.info("Attempting to open file " + file_path.getName()
					+ " for reading");
			_fileMode = FILE_MODE_READ;
			openForRead(file_path);
		} else {
			throw new IOException("File " + file_path.getName()
					+ " already opened for reading/writing");
		}
	}

	/**
	 * Method for opening a file for the purposes of writing. The function
	 * {@link AbstractImageBundle#open(OutputStream)} contains the necessary
	 * checks to determine whether a file can be opened for writing.
	 * 
	 * @param output_stream
	 *            the stream that will be written to
	 * @throws IOException
	 */
	/**
	 * TODO: Decide whether the subclasses should receive Paths or
	 * DataOutputStreams. In the former case, the subclass has to bother with
	 * actually opening the file. The issue here is that some implementations
	 * might have specific ways of actually opening the files (maybe the HAR for
	 * example). We need to follow-up on this.
	 */
	protected abstract void openForWrite(Path output_file) throws IOException;

	/**
	 * Method for opening a file for the purposes of reading. The function
	 * {@link AbstractImageBundle#open(InputStream)} contains the necessary
	 * checks to determine whether a file can be opened for reading.
	 * 
	 * @param input_stream
	 *            the stream that will be read from
	 * @throws IOException
	 */
	protected abstract void openForRead(Path input_file) throws IOException;

	/**
	 * Add an image to this bundle. Some implementations may not actually write
	 * the data to the file system until after close has been called.
	 * 
	 * @param image
	 *            the image to add to this bundle
	 * @throws IOException
	 */
	public final void addImage(FloatImage image) throws IOException {
		addImage(image, JPEGImageUtil.getInstance());
	}
	
	public final void addImage(FloatImage image, ImageEncoder encoder) throws IOException {
		addImage(image, encoder, encoder.createSimpleHeader(image));
	}
	
	public final void addImage(FloatImage image, ImageEncoder encoder, ImageHeader header) throws IOException {
		PipedOutputStream pipe_out = new PipedOutputStream();
		PipedInputStream pipe_in = new PipedInputStream(pipe_out);
		
		encoder.encodeImageHeader(header, pipe_out);
		encoder.encodeImage(image, pipe_out);
		
		addImage(pipe_in, header.getImageType());
	}
	
	public abstract void addImage(InputStream image_stream, ImageType type) throws IOException;

	/**
	 * Get the number of images contained in this bundle
	 * 
	 * @return
	 */
	public abstract long getImageCount();
	
	protected abstract ImageHeader readNextHeader() throws IOException;
	protected abstract FloatImage readNextImage() throws IOException;

	public final ImageHeader nextHeader() throws IOException {
		if (!_readHeader) {
			_readHeader = true;
			_readImage = false;
			return readNextHeader();
		} else {
			throw new IOException("Header already read");
		}
	}
	
	public final FloatImage nextImage() throws IOException {
		if (!_readHeader) {
			nextHeader();
		}
		
		if (!_readImage) {
			_readImage = true;
			_readHeader = false;
			return readNextImage();
		} else {
			throw new IOException("Image already read");
		}
	}

	/**
	 * Returns a boolean indicating whether there are more images left to read
	 * from this bundle.
	 * 
	 * @return
	 */
	public abstract boolean hasNext();

	/**
	 * Closes the underlying stream for this bundle. In some implementations, no
	 * data is written to the output stream unless this function is called.
	 * 
	 * @throws IOException
	 */
	public abstract void close() throws IOException;
}
