package hipi.image.io;

import hipi.image.ImageHeader.ImageFormat;
import hipi.image.io.JpegCodec;
import hipi.image.io.PngCodec;
import hipi.image.io.PpmCodec;

/**
 * This class manages choosing codecs for the different image formats.
 */
public final class CodecManager {

  static public ImageDecoder getDecoder(ImageFormat format) {
    ImageDecoder decoder = null;
    switch (format) {
    case JPEG:
      decoder = JpegCodec.getInstance();
      break;
    case PNG:
      decoder = PngCodec.getInstance();
      break;
    case PPM:
      decoder = PpmCodec.getInstance();
      break;
    }
    return decoder;
  }

  static public ImageEncoder getEncoder(ImageFormat format) {
    ImageEncoder encoder = null;
    switch (format) {
    case JPEG:
      encoder = JpegCodec.getInstance();
      break;
    case PNG:
      encoder = PngCodec.getInstance();
      break;
    case PPM:
      encoder = PpmCodec.getInstance();
      break;
    }
    return encoder;
  }

}
