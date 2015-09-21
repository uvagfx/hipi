package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.RasterImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.PixelArray;

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

/**
 * Extends {@link ImageCodec} and serves as both an {@link ImageDecoder} and 
 * {@link ImageEncoder} for the JPEG image storage format.
 */
public class JpegCodec extends ImageCodec {

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

    ImageWriteParam param = writer.getDefaultWriteParam();
    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    param.setCompressionQuality(0.95F); // highest JPEG quality = 1.0F

    encodeRasterImage((RasterImage)image, writer, param);
  }

}
