package hipi.image.io;

import java.io.IOException;
import java.io.InputStream;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

/**
 * This class provides the necessary functions for reading an image from an
 * InputStream. All subclasses must contain methods that know how to read the
 * image header.
 * 
 */
public interface ImageDecoder {
	
	public ImageHeader decodeImageHeader(InputStream is) throws IOException;
	public FloatImage decodeImage(InputStream is) throws IOException;

}
