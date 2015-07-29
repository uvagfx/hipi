package hipi.image.convert;

import hipi.image.HipiImageHeader;
import hipi.image.ByteImage;
import hipi.image.HipiImage;
import hipi.image.HipiImageFactory;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class provides a single static function for converting between
 * different image encodings.
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
   * @see hipi.image.io.JpegCodec
   * @see hipi.image.io.PngCodec
   * @see hipi.image.io.PpmCodec
   */
  public static void convert(InputStream inputStream, ImageDecoder decoder,
			     OutputStream outputStream, ImageEncoder encoder) throws IOException {
    ByteImage image = (ByteImage)decoder.decodeHeaderAndImage(inputStream, HipiImageFactory.getByteImageFactory(), true);
    encoder.encodeImage(image, outputStream);
  }

}
