package hipi.image.convert;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;

public class ImageConverter {

	/**
	 * Convert between two different image types
	 * 
	 * @param image_in an InputStream containing the image to convert
	 * @param image_out an OutputStream where the converted image will be written
	 * @param decoder_in the decoder for decoding the image_in
	 * @param encoder_out the encoder for encoding image_out
	 * 
	 * @throws IOException
	 * 
	 * @see hipi.image.io.JPEGImageUtil
	 * @see hipi.image.io.PNGImageUtil
	 * @see hipi.image.io.PPMImageUtil
	 */
	public static void convert(
			InputStream image_in, OutputStream image_out, 
			ImageDecoder decoder_in, ImageEncoder encoder_out) throws IOException {

		ImageHeader header = decoder_in.decodeImageHeader(image_in);
		FloatImage float_image = decoder_in.decodeImage(image_in);
		encoder_out.encodeImage(float_image, header, image_out);
	}
}
