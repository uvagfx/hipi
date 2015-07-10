package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

public class PpmCodec implements ImageDecoder, ImageEncoder {
  
  private static final PpmCodec staticObject = new PpmCodec();
	
  public static PpmCodec getInstance() {
    return staticObject;
  }

  private class PpmHeader {

    public int width;
    public int height;
    public int maxValue;
    public List<String> comments;

    public static PpmHeader decode(InputStream inputStream) throws IOException {

      byte[] header = new byte[255];
      inputStream.read(header);
      
      // Only P6 supported.
      if (header[0] != 'P' || header[1] != '6') {
	throw new IOException(String.format("PPM file has invalid or unsupported format [%c%c]. Only P6 is currently supported.", header[0], header[1]));
      }
      int off = 3;
      
      PpmHeader ppmHeader = new PpmHeader();
      
      // TODO: Fix this totally broken way to parse header. It assumes
      // the header ends after the third integer is parsed. This ignores
      // valid comment structure.
      
      ppmHeader.width = 0;
      while (header[off] >= '0' && header[off] <= '9')
	ppmHeader.width = ppmHeader.width * 10 + (header[off++] - '0');
      off++;
      
      ppmHeader.height = 0;
      while (header[off] >= '0' && header[off] <= '9')
	ppmHeader.height = ppmHeader.height * 10 + (header[off++] - '0');
      off++;
      
      ppmHeader.maxValue = 0;
      while (header[off] >= '0' && header[off] <= '9')
	ppmHeader.maxValue = ppmHeader.maxValue * 10 + (header[off++] - '0');
      off++;
      
      // TODO: Add support for extracting header comments and return in
      // List<String> comments field.
      
      return ppmHeader;
    }

  } // private class PpmHeader

  public ImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException, IllegalArgumentException {

    PpmHeader ppmHeader = internalDecodeHeader(inputStream);

    if (ppmHeader.maxValue != 255) {
      throw new IOException(String.format("Only 8-bit PPMs are currently supported. Max value reported in PPM header is [%d].", ppmHeader.maxValue));
    }

    if (includeExifData) {
      // TODO: Eventually, populate exifData map with comments
      // extracted from PPM header.
      throw new IllegalArgumentException("Support for extracting EXIF data from PPM files not implemented.");
    }

    return new ImageHeader(ImageFormat.PPM, ColorSpace.RGB,
			   ppmHeader.width, ppmHeader.height, 3, null, null);
  }

  public HipiImage decodeImage(InputStream inputStream, ImageHeader imageHeader, HipiImageFactory imageFactory) throws IllegalArgumentException, IOException {

    if (!(imageFactory.getType() == HipiImageType.FLOAT || imageFactory.getType() == HipiImageType.BYTE)) {
      throw new IllegalArgumentException("PPM decoder supports only FloatImage and ByteImage output types.");
    }

    ImageHeader checkHeader = decodeHeader(inputStream, false);

    // Check that image dimensions in header match those in JPEG
    if (checkHeader.width != imageHeader.getWidth() || checkHeader.height != imageHeader.getHeight()) {
      throw new IllegalArgumentException("Image dimensions in header do not match those in PPM.");
    }

    if (checkHeader.getNumBands() != imageHeader.getNumBands()) {
      throw new IllegalArgumentException("Number of image bands specified in header does not match number found in PPM.");
    }

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
    is.read(rest);

    for (int i = 0; i < 255 - off; i++) {
      ps.setElem(i, header[i + off] & 0xff);
    }
    for (int i = 0; i < w * h * 3 - (255 - off); i++) {
      pa.setElem(i + 255 - off, rest[i] & 0xff);
    }

    return image;
  }

  public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

    if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
      throw new IllegalArgumentException("PPM encoder supports only RasterImage input types.");
    }    

    if (image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Invalid image resolution.");
    }
    if (image.getColorSpace() != ColorSpace.RGB) {
      throw new IllegalArgumentException("PPM encoder supports only RGB color space.");
    }
    if (image.getNumBands() != 3) {
      throw new IllegalArgumentException("PPM encoder supports only three band images.");
    }

    int w = image.getWidth();
    int h = image.getHeight();

    PrintWriter writer = new PrintWriter(outputStream);
    writer.println("P6");
    writer.println(w + " " + h);
    writer.println("255");
    writer.flush();

    PixelArray pa = ((RasterImage)image).getPixelArray();

    byte[] raw = new byte[w*h*3];
    for (int i=0; i<w*h*3; i++) {
      raw[i] = (byte)pa.getElem(i);
    }

    outputStream.write(raw);

  }

}
