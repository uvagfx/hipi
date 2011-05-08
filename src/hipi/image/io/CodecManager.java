package hipi.image.io;

import hipi.image.ImageHeader.ImageType;

/**
 * This class contains the decoder and encoder classes for converting between image types
 *
 */
public final class CodecManager {
	static public ImageDecoder getDecoder(ImageType type) {
		ImageDecoder decoder = null;
		switch (type)
		{
		case JPEG_IMAGE:
			decoder = JPEGImageUtil.getInstance();
			break;
		case PPM_IMAGE:
			decoder = PPMImageUtil.getInstance();
			break;
		case PNG_IMAGE:
			decoder = PNGImageUtil.getInstance();
			break;
		}
		return decoder;
	}

	static public ImageEncoder getEncoder(ImageType type) {
		ImageEncoder encoder = null;
		switch (type)
		{
		case JPEG_IMAGE:
			encoder = JPEGImageUtil.getInstance();
			break;
		case PPM_IMAGE:
			encoder = PPMImageUtil.getInstance();
			break;
		case PNG_IMAGE:
			encoder = PNGImageUtil.getInstance();
			break;
		}
		return encoder;
	}
}
