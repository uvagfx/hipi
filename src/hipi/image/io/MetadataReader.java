package hipi.image.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.drew.imaging.jpeg.JpegMetadataReader;
import com.drew.imaging.jpeg.JpegProcessingException;
import com.drew.metadata.Metadata;

/**
 * Helper class for extracting JPEG EXIF data. 
 */
public class MetadataReader {

	private class UnclosableBufferedInputStream extends BufferedInputStream {

	    public UnclosableBufferedInputStream(InputStream in) {
	        super(in);
	    }

	    @Override
	    public void close() throws IOException {
	    }
	}
	
	private UnclosableBufferedInputStream _ubis;
	
	public MetadataReader(InputStream is) {
		_ubis = new UnclosableBufferedInputStream(is);
	}

	public Metadata extract() throws JpegProcessingException {
		return JpegMetadataReader.readMetadata(_ubis);
	}
}
