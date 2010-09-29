package hipi.image.io;

import hipi.image.RawImageHeader;

import java.io.InputStream;

public class JPEGImageReader extends ImageReader {

	public JPEGImageReader(InputStream image_stream) {
		super(image_stream);
	}

	@Override
	public RawImageHeader readImageHeader() {
		// TODO Auto-generated method stub
		return null;
	}

}
