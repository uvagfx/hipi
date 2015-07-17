package hipi.image.io;

import hipi.image.HipiImage;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides the necessary functions for encoding an image
 * and writing to a {@link OutputStream}.  All subclasses must contain
 * methods that know how to encode the image pixel data.
 */
public interface ImageEncoder {

  public abstract void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException;

}
