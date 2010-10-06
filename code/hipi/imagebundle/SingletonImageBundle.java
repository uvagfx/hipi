package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class SingletonImageBundle extends HipiImageBundle {

	private static final Log LOG = LogFactory.getLog(SingletonImageBundle.class
			.getName());
	
	private boolean _readOne = false;
	private boolean _wroteOne = false;

	public SingletonImageBundle(Configuration conf) {
		super(conf);
	}

	@Override
	public long getImageCount() {
		return 1;
	}

	@Override
	public boolean hasNext() {
		return !_readOne;
	}

	@Override
	public void addImage(InputStream image_stream, ImageType type)
			throws IOException {
		LOG.info("Adding image of type " + type);
		
		if (!_wroteOne) {
			super.addImage(image_stream, type);
		} else {
			throw new IOException("Already wrote one image");
		}
		
	}

	@Override
	public ImageHeader readNextHeader() throws IOException {
		if (!_readOne) {
			return super.readNextHeader();
		} else {
			throw new IOException("No more images available. You should consider using the function hasNext().");
		}
	}

	@Override
	public FloatImage readNextImage() throws IOException {
		if (!_readOne) {
			_readOne = true;
			return super.readNextImage();
		} else {
			throw new IOException("No more images available. You should consider using the function hasNext().");
		}
	}

}
