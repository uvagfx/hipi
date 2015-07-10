package hipi.image.io;

import com.drew.imaging.jpeg.JpegMetadataReader;
import com.drew.imaging.jpeg.JpegProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.HashMap;

/**
 * Helper class for extracting JPEG EXIF data. 
 */
public class ExifDataReader {

  private class UnclosableBufferedInputStream extends BufferedInputStream {

    public UnclosableBufferedInputStream(InputStream in) {
      super(in);
    }

    @Override
    public void close() throws IOException {
    }
  }
	
  private UnclosableBufferedInputStream ubis;
  
  public ExifDataReader(InputStream is) {
    ubis = new UnclosableBufferedInputStream(is);
  }
  
  public Metadata extract() throws JpegProcessingException, IOException {
    return JpegMetadataReader.readMetadata(ubis);
  }

  public static HashMap<String,String> extractAndFlatten(InputStream is) throws IOException {
    HashMap<String,String> exifData = new HashMap<String,String>();
    try {
      ExifDataReader reader = new ExifDataReader(is);
      Metadata metadata = reader.extract();
      Iterator directories = metadata.getDirectories().iterator();
      while (directories.hasNext()) {
	Directory directory = (Directory)directories.next();
	Iterator tags = directory.getTags().iterator();
	while (tags.hasNext()) {
	  Tag tag = (Tag)tags.next();
	  exifData.put(tag.getTagName(), tag.getDescription());
	}
      }
    } catch (JpegProcessingException ex) {
      throw new IOException(String.format("Encountered error while reading image EXIF data [%s]", ex.getMessage()));
    }
    return exifData;
  }

}
