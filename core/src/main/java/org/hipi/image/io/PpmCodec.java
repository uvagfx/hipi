package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.RasterImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.PixelArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * Extends {@link ImageCodec} and serves as both an {@link ImageDecoder} and 
 * {@link ImageEncoder} for the PPM image storage format.
 */
public class PpmCodec extends ImageCodec {
  
  private static final PpmCodec staticObject = new PpmCodec();
	
  public static PpmCodec getInstance() {
    return staticObject;
  }

  private class PpmHeader {

    public int width;
    public int height;
    public int numBands;
    public int maxValue;
    public ArrayList<String> comments = new ArrayList<String>();
    public int streamOffset; // byte offset to start of image data
    public byte[] headerBytes = new byte[255];

  } // private class PpmHeader

  private PpmHeader internalDecodeHeader(InputStream inputStream) throws IOException {

    PpmHeader ppmHeader = new PpmHeader();
    ppmHeader.numBands = 3;

    inputStream.read(ppmHeader.headerBytes);
    
    // Only P6 supported.
    if (ppmHeader.headerBytes[0] != 'P' || ppmHeader.headerBytes[1] != '6') {
      byte[] format = new byte[2];
      format[0] = ppmHeader.headerBytes[0];
      format[1] = ppmHeader.headerBytes[1];
      throw new IOException(String.format("PPM file has invalid or unsupported format [%s]. Only P6 is currently supported.", new String(format, "UTF-8")));
    }
    
    int off = 3;
    
    // TODO: Fix this totally broken way to parse header. It assumes
    // the header ends after the third integer is parsed. This ignores
    // valid comment structure.
    
    ppmHeader.width = 0;
    while (ppmHeader.headerBytes[off] >= '0' && 
	   ppmHeader.headerBytes[off] <= '9') {
      ppmHeader.width = ppmHeader.width * 10 + (ppmHeader.headerBytes[off++] - '0');
    }
    off++;

    ppmHeader.height = 0;
    while (ppmHeader.headerBytes[off] >= '0' && 
	   ppmHeader.headerBytes[off] <= '9') {
      ppmHeader.height = ppmHeader.height * 10 + (ppmHeader.headerBytes[off++] - '0');
    }
    off++;

    ppmHeader.maxValue = 0;
    while (ppmHeader.headerBytes[off] >= '0' && 
	   ppmHeader.headerBytes[off] <= '9') {
      ppmHeader.maxValue = ppmHeader.maxValue * 10 + (ppmHeader.headerBytes[off++] - '0');
    }
    off++;

    ppmHeader.streamOffset = off;
    
    // TODO: Add support for extracting header comments and return in
    // List<String> comments field.
    
    return ppmHeader;

  }

  public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) 
    throws IOException, IllegalArgumentException {

    PpmHeader ppmHeader = internalDecodeHeader(inputStream);

    if (ppmHeader.maxValue != 255) {
      throw new IOException(String.format("Only 8-bit PPMs are currently supported. Max value reported in PPM header is [%d].", ppmHeader.maxValue));
    }

    if (includeExifData) {
      // TODO: Eventually, populate exifData map with comments
      // extracted from PPM header.
      throw new IllegalArgumentException("Support for extracting EXIF data from PPM files not implemented.");
    }

    return new HipiImageHeader(HipiImageFormat.PPM, HipiColorSpace.RGB,
			       ppmHeader.width, ppmHeader.height, 3, null, null);
  }

  /*
  public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader,
			       HipiImageFactory imageFactory) throws IllegalArgumentException, IOException {

    if (!(imageFactory.getType() == HipiImageType.FLOAT || imageFactory.getType() == HipiImageType.BYTE)) {
      throw new IllegalArgumentException("PPM decoder supports only FloatImage and ByteImage output types.");
    }

    PpmHeader ppmHeader = internalDecodeHeader(inputStream);

    if (ppmHeader.maxValue != 255) {
      throw new IOException(String.format("Only 8-bit PPMs are currently supported. Max value reported in PPM header is [%d].", ppmHeader.maxValue));
    }

    // Check that image dimensions in header match those in JPEG
    if (ppmHeader.width != imageHeader.getWidth() || 
	ppmHeader.height != imageHeader.getHeight()) {
      throw new IllegalArgumentException("Image dimensions in header do not match those in PPM.");
    }

    if (ppmHeader.numBands != imageHeader.getNumBands()) {
      throw new IllegalArgumentException("Number of image bands specified in header does not match number found in PPM.");
    }

    int off = ppmHeader.streamOffset;

    // Create output image
    RasterImage image = null;
    try {
      image = (RasterImage)imageFactory.createImage(imageHeader);
    } catch (Exception e) {
      System.err.println(String.format("Unrecoverable exception while creating image object [%s]", e.getMessage()));
      e.printStackTrace();
      System.exit(1);
    }

    PixelArray pa = image.getPixelArray();

    int w = imageHeader.getWidth();
    int h = imageHeader.getHeight();

    byte[] rest = new byte[w * h * 3 - (255 - off)];
    inputStream.read(rest);

    for (int i = 0; i < 255 - off; i++) {
      pa.setElemNonLinSRGB(i, ppmHeader.headerBytes[i + off] & 0xff);
    }

    for (int i = 0; i < w * h * 3 - (255 - off); i++) {
      pa.setElemNonLinSRGB(i + 255 - off, rest[i] & 0xff);
    }

    return image;
  }
  */

  public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

    if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
      throw new IllegalArgumentException("PPM encoder supports only RasterImage input types.");
    }    

    if (image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Invalid image resolution.");
    }
    if (image.getColorSpace() != HipiColorSpace.RGB) {
      throw new IllegalArgumentException("PPM encoder supports only linear RGB color space.");
    }
    if (image.getNumBands() != 3) {
      throw new IllegalArgumentException("PPM encoder supports only three band images.");
    }

    int w = image.getWidth();
    int h = image.getHeight();

    // http://netpbm.sourceforge.net/doc/ppm.html
    PrintWriter writer = new PrintWriter(outputStream);
    writer.print("P6\r");
    writer.print(w + " " + h + "\r");
    writer.print("255\r");
    writer.flush();

    PixelArray pa = ((RasterImage)image).getPixelArray();

    byte[] raw = new byte[w*h*3];
    for (int i=0; i<w*h*3; i++) {
      raw[i] = (byte)pa.getElemNonLinSRGB(i);
    }

    outputStream.write(raw);

  }

}
