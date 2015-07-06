package hipi.image.io;

import hipi.image.ImageHeader;
import hipi.image.HipiImage;
import hipi.image.HipiImageFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class provides the necessary functions for decoding an image
 * from an {@link InputStream}. All subclasses must contain methods
 * that know how to read and decode the image header and the image
 * pixel data.
 */
public interface ImageDecoder {

  public ImageHeader decodeHeader(InputStream inputStream) throws IOException;

  public HipiImage decodeImage(InputStream inputStream, ImageHeader imageHeader, HipiImageFactory imageFactory) throws IllegalArgumentException, IOException;

  //  public HipiImage decodeHeaderAndImage(InputStream imageStream, HipiImageFactory imageFactory) throws IOException, IllegalArgumentException;
  public default HipiImage decodeHeaderAndImage(InputStream inputStream, HipiImageFactory imageFactory) 
    throws IOException, IllegalArgumentException 
  {
    ImageHeader header = decodeHeader(inputStream);
    return decodeImage(inputStream, header, imageFactory);
  }

}
