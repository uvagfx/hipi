package hipi.image.io;

import hipi.image.HipiImageHeader;
import hipi.image.HipiImage;
import hipi.image.HipiImageFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

/**
 * This class provides the necessary functions for decoding an image
 * from an {@link InputStream}. All subclasses must contain methods
 * that know how to read and decode the image header and the image
 * pixel data.
 */
public interface ImageDecoder {

  public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException;

  public default HipiImageHeader decodeHeader(InputStream inputStream) 
    throws IOException {
    return decodeHeader(inputStream, false);
  }
  
  public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, 
			       HipiImageFactory imageFactory) throws IllegalArgumentException, IOException;

  public default HipiImage decodeHeaderAndImage(InputStream inputStream, HipiImageFactory imageFactory, boolean includeExifData) 
    throws IOException, IllegalArgumentException 
  {
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    bufferedInputStream.mark(Integer.MAX_VALUE);
    HipiImageHeader header = decodeHeader(bufferedInputStream, includeExifData);
    bufferedInputStream.reset();
    return decodeImage(bufferedInputStream, header, imageFactory);
  }

}
