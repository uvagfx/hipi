package hipi.image.convert;

import hipi.image.FloatImage;
import hipi.image.RawImage;

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
	public static RawImage convert(RawImage image, 
			RawImageConverter converterFrom, RawImageConverter converterTo) {
		FloatImage float_image = converterFrom.DecodeToFloatImage(image);
		
		return converterTo.EncodeFromFloatImage(float_image);
	}
}
