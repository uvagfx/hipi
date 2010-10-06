package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JPEGImageUtil implements ImageDecoder, ImageEncoder {
	
	private static final JPEGImageUtil static_object = new JPEGImageUtil();
	
	public static JPEGImageUtil getInstance() {
		return static_object;
	}

	public ImageHeader decodeImageHeader(InputStream is) throws IOException {
		return null;
	}

	public FloatImage decodeImage(InputStream is) throws IOException {
		return null;
	}

	public void encodeImageHeader(ImageHeader header, OutputStream os)
			throws IOException {
		
	}

	public void encodeImage(FloatImage image, OutputStream os)
			throws IOException {
		
	}

	public ImageHeader createSimpleHeader(FloatImage image) {
		return null;
	}

}
