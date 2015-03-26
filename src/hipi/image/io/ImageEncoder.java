package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.OutputStream;
/**
 * This class provides the necessary functions for writing an image from a FloatImage to an
 * OutputStream.
 * 
 */
public interface ImageEncoder {

	public ImageHeader createSimpleHeader(FloatImage image);
	public void encodeImage(FloatImage image, ImageHeader header, OutputStream os) throws IOException;
}
