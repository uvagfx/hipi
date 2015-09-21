package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

/**
 * Interface for decoding a {@link HipiImageHeader} and {@link HipiImage} from a Java
 * {@link java.io.InputStream}.
 */
public interface ImageDecoder {

  /**
   * Read and decode header for image accessed through a Java {@link java.io.InputStream}.
   * Optionally extracts image EXIF data, if available.
   *
   * @param inputStream input stream containing serialized image data
   * @param includeExifData if true attempts to extract image EXIF data
   *
   * @return image header data represented as a {@link HipiImageHeader}
   *
   * @throws IOException if an error is encountered while reading from input stream
   */
  public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData)
  throws IOException;

  /**
   * Read and decode header for image accessed through a Java {@link java.io.InputStream}.
   * Does not attempt to extract image EXIF data. Default implementation in {@link ImageCodec}
   * calls {@link ImageDecoder#decodeHeader} with includeExifData parameter set to false.
   *
   * @param inputStream input stream containing serialized image data
   *
   * @return image header data represented as a {@link HipiImageHeader}
   *
   * @throws IOException if an error is encountered while reading from input stream
   */
  public HipiImageHeader decodeHeader(InputStream inputStream) throws IOException;

  /**
   * Read and decode image from a Java {@link java.io.InputStream}.
   *
   * @param inputStream input stream containing serialized image data
   * @param imageHeader image header that was previously initialized
   * @param imageFactory factory object capable of creating objects of desired HipiImage type
   * @param includeExifData if true attempts to extract image EXIF data
   *
   * @return image represented as a {@link HipiImage}
   *
   * @throws IllegalArgumentException if parameters are invalid or do not agree with image data
   * @throws IOException if an error is encountered while reading from the input stream
   */
  public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, 
			       HipiImageFactory imageFactory, boolean includeExifData)
    throws IllegalArgumentException, IOException;

  /**
   * Read and decode both image header and image pixel data from a Java {@link java.io.InputStream}.
   * Both of these decoded objects can be accessed through the {@link HipiImage} object returned
   * by this method. See default implementation in {@link ImageCodec}.
   *
   * @param inputStream input stream containing serialized image data
   * @param imageFactory factory object capable of creating objects of desired HipiImage type
   * @param includeExifData if true attempts to extract image EXIF data
   *
   * @return image represented as a {@link HipiImage}
   *
   * @throws IllegalArgumentException if parameters are invalid
   * @throws IOException if an error is encountered while reading from the input stream
   */
  public HipiImage decodeHeaderAndImage(InputStream inputStream,
            HipiImageFactory imageFactory, boolean includeExifData) 
    throws IOException, IllegalArgumentException;  

}
