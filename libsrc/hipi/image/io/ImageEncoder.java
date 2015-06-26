package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides the necessary functions for encoding an image
 * and writing to a {@link OutputStream}.  All subclasses must contain
 * methods that know how to encode the image pixel data.
 */
public interface ImageEncoder {

  public <T> void encodeImage(RasterImage<T> img, OutputStream outputStream) throws IllegalArgumentException, IOException;

}
