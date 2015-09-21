package org.hipi.image.io;

import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PngCodec;
import org.hipi.image.io.PpmCodec;

/**
 * Finds a suitable {@link ImageEncoder} or {@link ImageDecoder} for a specific
 * {@link HipiImageFormat}.
 */
public final class CodecManager {

  /**
   * Find a {@link ImageDecoder} capable of deserializing a {@link HipiImage} object stored in a
   * specific {@link HipiImageFormat}.
   *
   * @param format storage format to assume during deserialization
   *
   * @return image decoder object
   *
   * @throws IllegalArgumentException if format is invalid or currently unsupported
   */
  static public ImageDecoder getDecoder(HipiImageFormat format) throws IllegalArgumentException {
    switch (format) {
    case JPEG:
      return JpegCodec.getInstance();
    case PNG:
      return PngCodec.getInstance();
    case PPM:
      return PpmCodec.getInstance();
    default:
      throw new IllegalArgumentException("Image format currently unsupported.");
    }
  }
  
  /**
   * Find a {@link ImageEncoder} capable of serializing a {@link HipiImage} to a target
   * {@link HipiImageFormat}.
   *
   * @param format storage format to target during serialization
   *
   * @return image encoder object
   *
   * @throws IllegalArgumentException if format is invalid or currently unsupported
   */
  static public ImageEncoder getEncoder(HipiImageFormat format) throws IllegalArgumentException {
    switch (format) {
    case JPEG:
      return JpegCodec.getInstance();
    case PNG:
      return PngCodec.getInstance();
    case PPM:
      return PpmCodec.getInstance();
    default:
      throw new IllegalArgumentException("Image format currently unsupported.");
    }
  }

}
