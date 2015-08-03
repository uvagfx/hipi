package org.hipi.image.io;

import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PngCodec;
import org.hipi.image.io.PpmCodec;

/**
 * This class manages choosing codecs for the different image formats.
 */
public final class CodecManager {

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
