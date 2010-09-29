package hipi.image;

/**
 * A RawImage is an image that is usually created from a file read. It contains
 * no specific information about the image apart from the bytes that comprise
 * it. Before using the image, it must be decoded with an
 * {@link RawImageConverter} into a usable format such as a {@link FloatImage}
 * or a {@link UCharImage}.
 * 
 * In general this class should not be instantiated directly. Instead, something
 * like the {@link ImageReader} class should be used to create this class as the
 * result of a read operation from disk.
 * 
 * @see ImageReader, RawImageConverter
 * 
 * @author seanarietta
 * 
 */
public class RawImage {

	private RawImageHeader _header;
	private byte[] _data;
	
	public RawImage(byte[] data) {
		this(null, data);
	}
	
	public RawImage(RawImageHeader header, byte[] data){
		_header = header;
		_data = data;
	}
	
	public byte[] getRawData() {
		return _data;
	}
	
	public RawImageHeader getHeader() {
		return _header;
	}
}
