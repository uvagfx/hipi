package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.OutputStream;

public interface ImageEncoder {

	public ImageHeader createSimpleHeader(FloatImage image);
	public void encodeImage(FloatImage image, ImageHeader header, OutputStream os) throws IOException;
}
