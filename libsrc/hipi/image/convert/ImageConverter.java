package hipi.image.convert;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;

/**
 * This class provides a single static function for converting between different image encodings.
 */
public class ImageConverter {

  /**
   * Convert between two different image formats.
   * 
   * @param imageIn an InputStream containing the image to convert
   * @param imageOut an OutputStream where the converted image will be written
   * @param decoderIn the decoder for decoding the image_in
   * @param encoderOut the encoder for encoding image_out
   * 
   * @throws IOException
   * 
   * @see hipi.image.io.JPEGImageUtil
   * @see hipi.image.io.PNGImageUtil
   * @see hipi.image.io.PPMImageUtil
   */
  public static void convert(InputStream imageIn, OutputStream imageOut, ImageDecoder decoderIn,
      ImageEncoder encoderOut) throws IOException {
    ImageHeader header = decoderIn.decodeImageHeader(imageIn);
    FloatImage image = decoderIn.decodeImage(imageIn);
    encoderOut.encodeImage(image, header, imageOut);
  }
}
