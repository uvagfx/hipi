package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

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

  @SuppressWarnings("rawtypes")
  public ImageHeader decodeImageHeader(InputStream is) throws IOException {

    try {
      DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
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
      
      ImageHeader header = new ImageHeader(ImageType.JPEG, ColorSpace.RGB, 
					   width, height, depth, 3, null);

      MetadataReader reader = new MetadataReader(dis);
      Metadata metadata = reader.extract();
      Iterator directories = metadata.getDirectories().iterator();
      while (directories.hasNext()) {
	Directory directory = (Directory) directories.next();
	Iterator tags = directory.getTags().iterator();
	while (tags.hasNext()) {
	  Tag tag = (Tag) tags.next();
	  //	  header.addEXIFInformation(tag.getTagName(), tag.getDescription());
	  header.addMetaData(tag.getTagName(), tag.getDescription());
	}
      }
    } catch (Exception e) {
    }
    return header;
  }

  public <T> void decodeImage(InputStream is, RasterImage<T> img) throws IllegalArgumentException, IOException {

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
    ImageInputStream iis = ImageIO.createImageInputStream(is);
    reader.setInput(iis);

    // Decode JPEG and obtain pointer to raster image data
    Raster raster = reader.readRaster(0, new ImageReadParam());
    DataBuffer dataBuffer = raster.getDataBuffer();
    int w = raster.getWidth();
    int h = raster.getHeight();

    // Check that image dimensions in header match those in JPEG
    if (w != img.getWidth() || h != img.getHeight()) {
      throw new IllegalArgumentException("Image dimensions in header do not match those in JPEG.");
    }

    // Convert to desired pixel type
    T[] pels = img.getData();
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
	  pels[i * w * 3 + j * 3 + 0] = img.convertFromInt(k - (c * k >> 8));
	  pels[i * w * 3 + j * 3 + 1] = img.convertFromInt(k - (m * k >> 8));
	  pels[i * w * 3 + j * 3 + 2] = img.convertFromInt(k - (y * k >> 8));
	  
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
	  pels[i * w * 3 + j * 3 + 0] = img.convertFromInt(Math.min(Math.max(Y + 1.772f * (Cb - 128), 0), 255));
	  pels[i * w * 3 + j * 3 + 1] = img.convertFromInt(Math.min(Math.max(Y - 0.34414f * (Cb - 128) - 0.71414f * (Cr - 128), 0), 255));
	  pels[i * w * 3 + j * 3 + 2] = img.convertFromInt(Math.min(Math.max(Y + 1.402f * (Cr - 128), 0), 255));
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
	  pels[i * w * 3 + j * 3 + 0] = img.convertFromInt(Y);
	  pels[i * w * 3 + j * 3 + 1] = img.convertFromInt(Y);
	  pels[i * w * 3 + j * 3 + 2] = img.convertFromInt(Y);
	}
      }
    }    
  }

  public <T> void encodeImage(OutputStream os, RasterImage<T> img) throws IllegalArgumentException, IOException {

    if (img.getWidth() <= 0 || img.getHeight() <= 0) {
      throw new IllegalArgumentException("Image must have non-zero size.");
    }
    if (img.getColorSpace() != ColorSpace.RGB) {
      throw new IllegalArgumentException("Only RGB color space currently supported.");
    }
    if (img.getNumBands() != 3) {
      throw new IllegalArgumentException("Only three band images currently supported.");
    }

    // Find suitable JPEG writer in javax.imageio.ImageReader
    ImageOutputStream ios = ImageIO.createImageOutputStream(os);
    Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
    ImageWriter writer = writers.next();
    writer.setOutput(ios);

    int w = img.getWidth();
    int h = img.getHeight();

    BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

    T[] data = img.getData();
    int[] rgb = new int[w*h];
    for (int i=0; i<w*h; i++) {
      /*
      int r = Math.min(Math.max((int) (data[i*3+0] * 255), 0), 255);
      int g = Math.min(Math.max((int) (data[i*3+1] * 255), 0), 255);
      int b = Math.min(Math.max((int) (data[i*3+2] * 255), 0), 255);
      */
      int r = img.convertToInt(data[i*3+0]);
      int g = img.convertToInt(data[i*3+1]);
      int b = img.convertToInt(data[i*3+2]);
      rgb[i] = (r << 16) | (g << 8) | b;
    }
    bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);
    IIOImage iioImage = new IIOImage(bufferedImage, null, null);
    ImageWriteParam param = writer.getDefaultWriteParam();
    writer.write(null, iioImage, param);
  }

}
