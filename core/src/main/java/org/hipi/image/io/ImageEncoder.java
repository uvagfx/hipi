package org.hipi.image.io;

import org.hipi.image.HipiImage;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface for encoding a {@link HipiImage} in a particular storage format and writing the
 * result to a Java {@link java.io.OutputStream}.
 */
public interface ImageEncoder {

  /**
   * Encode and write image to a Java {@link java.io.OutputStream}.
   *
   * @param image source image to be encoded
   * @param outputStream output stream that will receive encoded image
   *
   * @throws IllegalArgumentException if parameters are null or otherwise invalid
   * @throws IOException if an error is encountered during stream serialization
   */
  public abstract void encodeImage(HipiImage image, OutputStream outputStream)
  throws IllegalArgumentException, IOException;

}
