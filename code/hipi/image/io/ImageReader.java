package hipi.image.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import hipi.image.RawImage;
import hipi.image.RawImageHeader;

/**
 * This class provides the necessary functions for reading an image from an
 * InputStream. All subclasses must contain methods that know how to read the
 * image header.
 * 
 * @author seanarietta
 */
public abstract class ImageReader {

	private final static int READ_BUFFER_SIZE = 1024;

	protected RawImageHeader _header;
	protected InputStream _image_stream;

	public ImageReader(InputStream image_stream) {
		_image_stream = image_stream;
	}

	public abstract RawImageHeader readImageHeader() throws IOException;

	/**
	 * Reads an image into a RawImage. This function will automatically read the
	 * header even if the function readImageHeader has not been called.
	 * 
	 * @return
	 * @throws IOException
	 */
	public final RawImage readImage() throws IOException {
		if (_header == null) {
			_header = readImageHeader();
			if (_header == null) {
				throw new IOException("Could not read header from InputStream");
			}
		}

		// Image header guaranteed to be read if we are here

		// TODO: This messy business with the byte stream could be avoided if we
		// knew the size of the file. Then we could just allocate a byte array
		// of the correct size and read in the data. Unfortunately,
		// InputStream's don't have that kind of information in them.
		byte[] raw_buffer = new byte[READ_BUFFER_SIZE];
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int read_bytes;
		while ((read_bytes = _image_stream.read(raw_buffer)) != -1) {
			buffer.write(raw_buffer, 0, read_bytes);
		}

		return new RawImage(_header, buffer.toByteArray());
	}

}
