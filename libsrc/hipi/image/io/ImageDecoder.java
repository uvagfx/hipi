package hipi.image.io;

import java.io.IOException;
import java.io.InputStream;

import hipi.image.ImageHeader;
import hipi.image.RasterImage;

/**
 * This class provides the necessary functions for decoding an image
 * from an {@link InputStream}. All subclasses must contain methods
 * that know how to read and decode the image header and the image
 * pixel data.
 */
public interface ImageDecoder {

  public ImageHeader decodeImageHeader(InputStream is) throws IOException;

  public <T> boolean decodeImage(InputStream imageStream, RasterImage<T> image) throws IllegalArgumentException, IOException;

}
