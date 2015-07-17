package hipi.image.io;

import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.image.HipiImageHeader.HipiColorSpace;
import hipi.image.HipiImage;
import hipi.image.HipiImage.HipiImageType;
import hipi.image.RasterImage;
import hipi.image.HipiImageFactory;
import hipi.image.PixelArray;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.HashMap;

import javax.imageio.IIOImage;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

public class JpegCodec extends ImageCodec { //implements ImageDecoder, ImageEncoder {

  private static final JpegCodec staticObject = new JpegCodec();

  public static JpegCodec getInstance() {
    return staticObject;
  }

  public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) 
    throws IOException, IllegalArgumentException {

    DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
    dis.mark(Integer.MAX_VALUE);
      
    // all JPEGs start with -40
    short magic = dis.readShort();
    if (magic != -40)
      return null;

    int width=0, height=0, depth=0;
    
    byte[] data = new byte[6];
      
    // read in each block to determine resolution and bit depth
    for (;;) {
      dis.read(data, 0, 4);
      if ((data[0] & 0xff) != 0xff)
	return null;
      if ((data[1] & 0xff) == 0x01 || ((data[1] & 0xff) >= 0xd0 && (data[1] & 0xff) <= 0xd7))
	continue;
      long length = (((data[2] & 0xff) << 8) | (data[3] & 0xff)) - 2;
      if ((data[1] & 0xff) == 0xc0 || (data[1] & 0xff) == 0xc2) {
	dis.read(data);
	height = ((data[1] & 0xff) << 8) | (data[2] & 0xff);
	width = ((data[3] & 0xff) << 8) | (data[4] & 0xff);
	depth = data[0] & 0xff;
	break;
      } else {
	while (length > 0) {
	  long skipped = dis.skip(length);
	  if (skipped == 0)
	    break;
	  length -= skipped;
	}
      }
    }
    
    if (depth != 8) {
      throw new IllegalArgumentException(String.format("Image has unsupported bit depth [%d].", depth));
    }

    HashMap<String,String> exifData = null;
    if (includeExifData) {
      dis.reset();
      exifData = ExifDataReader.extractAndFlatten(dis);
    }
    
    return new HipiImageHeader(HipiImageFormat.JPEG, HipiColorSpace.RGB, 
			       width, height, 3, null, exifData);
  }

  /*
  public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, 
			       HipiImageFactory imageFactory) throws IllegalArgumentException, IOException {

    // Verify image factory
    if (!(imageFactory.getType() == HipiImageType.FLOAT || imageFactory.getType() == HipiImageType.BYTE)) {
      throw new IllegalArgumentException("JPEG decoder supports only FloatImage and ByteImage output types.");
    }

    // Use TwelveMonkeys ImageIO plugin decoder
    BufferedImage javaImage = ImageIO.read(inputStream);
    int w = javaImage.getWidth();
    int h = javaImage.getHeight();

    // Check that image dimensions in header match those in JPEG
    if (w != imageHeader.getWidth() || h != imageHeader.getHeight()) {
      throw new IllegalArgumentException("Image dimensions in header do not match those in JPEG.");
    }

    // Create output image
    RasterImage image = null;
    try {
      image = (RasterImage)imageFactory.createImage(imageHeader);
    } catch (Exception e) {
      System.err.println(String.format("Fatal error while creating image object [%s]", e.getMessage()));
      e.printStackTrace();
      System.exit(1);
    }

    PixelArray pa = image.getPixelArray();
    
    for (int j=0; j<h; j++) {
      for (int i=0; i<w; i++) {

	// Retrieve 8-bit non-linear sRGB value packed into int
	int pixel = javaImage.getRGB(i,j); 

	int red = (pixel >> 16) & 0xff;
	int grn = (pixel >>  8) & 0xff;
	int blu = (pixel      ) & 0xff;

	// Set value in pixel array using routine designed for sRGB values
	pa.setElemNonLinSRGB((j*w+i)*3+0, red);
	pa.setElemNonLinSRGB((j*w+i)*3+1, grn);
	pa.setElemNonLinSRGB((j*w+i)*3+2, blu);

      }
    }

    return image;
  }
  */

  public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

    if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
      throw new IllegalArgumentException("JPEG encoder supports only RasterImage input types.");
    }    

    if (image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Invalid image resolution.");
    }
    if (image.getColorSpace() != HipiColorSpace.RGB) {
      throw new IllegalArgumentException("JPEG encoder supports only RGB color space.");
    }
    if (image.getNumBands() != 3) {
      throw new IllegalArgumentException("JPEG encoder supports only three band images.");
    }

    // Find suitable JPEG writer in javax.imageio.ImageReader
    ImageOutputStream ios = ImageIO.createImageOutputStream(outputStream);
    Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
    ImageWriter writer = writers.next();
    System.out.println("Using JPEG encoder: " + writer);
    writer.setOutput(ios);

    /*
    int w = image.getWidth();
    int h = image.getHeight();

    BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

    PixelArray pa = ((RasterImage)image).getPixelArray();
    int[] rgb = new int[w*h];
    for (int i=0; i<w*h; i++) {

      int r = pa.getElemNonLinSRGB(i*3+0);
      int g = pa.getElemNonLinSRGB(i*3+1);
      int b = pa.getElemNonLinSRGB(i*3+2);

      rgb[i] = (r << 16) | (g << 8) | b;
    }
    bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);
    IIOImage iioImage = new IIOImage(bufferedImage, null, null);
    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionQuality(0.95F); // highest quality = 1.0F
    writer.write(null, iioImage, param);
    */

    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionQuality(0.95F); // highest JPEG quality = 1.0F

    encodeRasterImage((RasterImage)image, writer, param);
  }

}
