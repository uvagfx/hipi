package hipi.image.convert;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import hipi.image.FloatImage;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;

public class ImageConverter {

	/**
	 * Converts between two raw image representations (JPEG, PNG, etc)
	 * @param image the image to convert
	 * @param converterFrom the representation that image is currently in
	 * @param converterTo the representation that image gets converted to
	 * @return a RawImage that is encoded with converterTo
	 * 
	 * @see JPEGImageConverter, PNGImageConverter, PPMImageConverter
	 */
	public static void convert(
			InputStream image_in, OutputStream image_out, 
			ImageDecoder decoder_in, ImageEncoder encoder_out) throws IOException {
		
		FloatImage float_image = decoder_in.decodeImage(image_in);
		encoder_out.encodeImage(float_image, image_out);
	}
}
