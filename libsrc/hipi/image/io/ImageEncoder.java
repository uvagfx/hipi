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

  //  public ImageHeader createSimpleHeader(FloatImage image);

  public <T> void encodeImage(OutputStream os, RasterImage<T> img) throws IllegalArgumentException, IOException;

}
