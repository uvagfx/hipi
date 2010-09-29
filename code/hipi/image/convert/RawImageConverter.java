package hipi.image.convert;

import hipi.image.FloatImage;
import hipi.image.RawImage;
import hipi.image.UCharImage;

/**
 * This interface should be implemented for any image file type that may be
 * found in the system. It provides method stubs for converting between raw
 * FloatImage/UCharImage and encoded FloatImage/UCharImage.
 * 
 * @see JPEGImageConverter, PNGImageConverter, PPMImageConverter
 * 
 * @author seanarietta
 * 
 */
public interface RawImageConverter {

	/**
	 * 
	 * @param image
	 * @return
	 */
	RawImage EncodeFromUCharImage(UCharImage image);

	/**
	 * 
	 * @param image
	 * @return
	 */
	RawImage EncodeFromFloatImage(FloatImage image);

	/**
	 * 
	 * @param image
	 * @return
	 */
	FloatImage DecodeToFloatImage(RawImage image);

	/**
	 * 
	 * @param image
	 * @return
	 */
	UCharImage DecodeToUCharImage(RawImage image);

}
