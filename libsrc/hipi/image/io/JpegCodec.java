package hipi.image.io;

import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageFormat;
import hipi.image.ImageHeader.ColorSpace;
import hipi.image.HipiImage;
import hipi.image.HipiImage.HipiImageType;
import hipi.image.RasterImage;
import hipi.image.HipiImageFactory;
import hipi.image.PixelArray;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

public class JpegCodec implements ImageDecoder, ImageEncoder {

  private static final JpegCodec staticObject = new JpegCodec();

  public static JpegCodec getInstance() {
    return staticObject;
  }

  public ImageHeader decodeHeader(InputStream inputStream) throws IOException, IllegalArgumentException {

    ImageHeader header = null;

    //    try {
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
      
      dis.reset();

      if (depth != 8) {
	throw new IllegalArgumentException(String.format("Image has unsupported bit depth [%d].", depth));
      }
      
      header = new ImageHeader(ImageFormat.JPEG, ColorSpace.RGB, 
			       width, height, 3, null, null);

      /*    } catch (Exception e) {
      System.err.println("Exception while decoding image header.");
    }
      */

    return header;
  }

  public HipiImage decodeImage(InputStream inputStream, ImageHeader imageHeader, HipiImageFactory imageFactory) throws IllegalArgumentException, IOException {

    if (!(imageFactory.getType() == HipiImageType.FLOAT || imageFactory.getType() == HipiImageType.BYTE)) {
      throw new IllegalArgumentException("JPEG decoder supports only FloatImage and ByteImage output types.");
    }
	
    // Find suitable JPEG reader in javax.imageio.ImageReader
    Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg");
    ImageReader reader = readers.next();
    if (!reader.canReadRaster()) {
      while (readers.hasNext()) {
	reader = readers.next();
	if (reader.canReadRaster())
	  break;
      }
    }

    // Setup input stream for reader
    ImageInputStream imageInputStream = ImageIO.createImageInputStream(inputStream);
    reader.setInput(imageInputStream);

    // Decode JPEG and obtain pointer to raster image data
    Raster raster = reader.readRaster(0, new ImageReadParam());
    DataBuffer dataBuffer = raster.getDataBuffer();
    int w = raster.getWidth();
    int h = raster.getHeight();

    // Check that image dimensions in header match those in JPEG
    if (w != imageHeader.getWidth() || h != imageHeader.getHeight()) {
      throw new IllegalArgumentException("Image dimensions in header do not match those in JPEG.");
    }

    if (raster.getNumBands() != imageHeader.getNumBands()) {
      throw new IllegalArgumentException("Number of image bands specified in header does not match number found in JPEG.");
    }

    // Create output image
    RasterImage image = null;
    try {
      image = (RasterImage)imageFactory.createImage(imageHeader);
    } catch (Exception e) {
      // TODO?!?
      System.err.println("CRASH");
      e.printStackTrace();
      System.exit(1);
    }

    // Convert to desired pixel type
    PixelArray pa = image.getPixelArray();
    if (raster.getNumBands() == 4) {
      for (int i = 0; i < h; i++) {
	for (int j = 0; j < w; j++) {
	  // Need reference
	  int Y  = dataBuffer.getElem(i * w * 4 + j * 4 + 0);
	  int Cr = dataBuffer.getElem(i * w * 4 + j * 4 + 1);
	  int Cb = dataBuffer.getElem(i * w * 4 + j * 4 + 2);
	  int c = (int) Math.min(Math.max(Y + 1.772 * (Cb - 128), 0), 255);
	  int m = (int) Math.min(Math.max(Y - 0.34414 * (Cb - 128) - 0.71414 * (Cr - 128), 0), 255);
	  int y = (int) Math.min(Math.max(Y + 1.402 * (Cr - 128), 0), 255);
	  int k = dataBuffer.getElem(i * w * 4 + j * 4 + 3);
	  /*
	  pels[i * w * 3 + j * 3 + 0] = (float) ((k - (c * k >> 8)) / 255.0);
	  pels[i * w * 3 + j * 3 + 1] = (float) ((k - (m * k >> 8)) / 255.0);
	  pels[i * w * 3 + j * 3 + 2] = (float) ((k - (y * k >> 8)) / 255.0);
	  */
	  pa.setElem(i * w * 3 + j * 3 + 0, (k - (c * k >> 8)));
	  pa.setElem(i * w * 3 + j * 3 + 1, (k - (m * k >> 8)));
	  pa.setElem(i * w * 3 + j * 3 + 2, (k - (y * k >> 8)));
	}
      }
    } else if (raster.getNumBands() == 3) {
      for (int i = 0; i < h; i++) {
	for (int j = 0; j < w; j++) {
	  // Need reference
	  int Y  = dataBuffer.getElem(i * w * 3 + j * 3 + 0);
	  int Cr = dataBuffer.getElem(i * w * 3 + j * 3 + 1);
	  int Cb = dataBuffer.getElem(i * w * 3 + j * 3 + 2);
	  /*
	  pels[i * w * 3 + j * 3 + 0] = (float) ((Math.min(Math.max(Y + 1.772f * (Cb - 128), 0), 255)) / 255.0);
	  pels[i * w * 3 + j * 3 + 1] = (float) ((Math.min(Math.max(Y - 0.34414f * (Cb - 128) - 0.71414f * (Cr - 128), 0), 255)) / 255.0);
	  pels[i * w * 3 + j * 3 + 2] = (float) ((Math.min(Math.max(Y + 1.402f * (Cr - 128), 0), 255)) / 255.0);
	  */
	  pa.setElem(i * w * 3 + j * 3 + 0, (int)Math.min(Math.max(Y + 1.772f * (Cb - 128), 0), 255));
	  pa.setElem(i * w * 3 + j * 3 + 1, (int)Math.min(Math.max(Y - 0.34414f * (Cb - 128) - 0.71414f * (Cr - 128), 0), 255));
	  pa.setElem(i * w * 3 + j * 3 + 2, (int)Math.min(Math.max(Y + 1.402f * (Cr - 128), 0), 255));
	}
      }
    } else if (raster.getNumBands() == 1) {
      for (int i = 0; i < h; i++) {
	for (int j = 0; j < w; j++) {
	  int Y = dataBuffer.getElem(i * w + j);
	  /*
	  pels[i * w * 3 + j * 3 + 0] = (float) (Y / 255.0);
	  pels[i * w * 3 + j * 3 + 1] = (float) (Y / 255.0);
	  pels[i * w * 3 + j * 3 + 2] = (float) (Y / 255.0);
	  */
	  pa.setElem(i * w * 3 + j * 3 + 0, Y);
	  pa.setElem(i * w * 3 + j * 3 + 1, Y);
	  pa.setElem(i * w * 3 + j * 3 + 2, Y);
	}
      }
    }

    return image;

  }

  public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

    if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
      throw new IllegalArgumentException("JPEG encoder supports only RasterImage input types.");
    }    

    if (image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Invalid image resolution.");
    }
    if (image.getColorSpace() != ColorSpace.RGB) {
      throw new IllegalArgumentException("JPEG encoder supports only RGB color space.");
    }
    if (image.getNumBands() != 3) {
      throw new IllegalArgumentException("JPEG encoder supports only three band images.");
    }

    // Find suitable JPEG writer in javax.imageio.ImageReader
    ImageOutputStream ios = ImageIO.createImageOutputStream(outputStream);
    Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
    ImageWriter writer = writers.next();
    writer.setOutput(ios);

    int w = image.getWidth();
    int h = image.getHeight();

    BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

    PixelArray pa = ((RasterImage)image).getPixelArray();
    int[] rgb = new int[w*h];
    for (int i=0; i<w*h; i++) {
      /*
      int r = Math.min(Math.max((int) (data[i*3+0] * 255), 0), 255);
      int g = Math.min(Math.max((int) (data[i*3+1] * 255), 0), 255);
      int b = Math.min(Math.max((int) (data[i*3+2] * 255), 0), 255);
      */
      int r = pa.getElem(i*3+0);
      int g = pa.getElem(i*3+1);
      int b = pa.getElem(i*3+2);
      rgb[i] = (r << 16) | (g << 8) | b;
    }
    bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);
    IIOImage iioImage = new IIOImage(bufferedImage, null, null);
    ImageWriteParam param = writer.getDefaultWriteParam();
    writer.write(null, iioImage, param);
  }

}
