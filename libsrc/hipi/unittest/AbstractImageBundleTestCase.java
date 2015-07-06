package hipi.unittest;

import static org.junit.Assert.*;

import hipi.image.HipiImage;
import hipi.image.HipiImageFactory;
import hipi.image.FloatImage;
import hipi.image.ByteImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageFormat;
import hipi.image.io.ImageDecoder;
import hipi.image.io.JpegCodec;
import hipi.imagebundle.AbstractImageBundle;

import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;

public abstract class AbstractImageBundleTestCase {

  public abstract AbstractImageBundle createImageBundleAndOpen(int mode, HipiImageFactory imageFactory) throws IOException;

  protected boolean setupFinished = false;

  @Before
  public void setup() throws IOException {
    if (setupFinished)
      return;
    setupFinished = true;

    System.out.println("AbstractImageBundle#setup");

    AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_WRITE, null);

    JpegCodec jpegCodec = JpegCodec.getInstance();

    File[] files = new File("./testimages").listFiles();

    for (File file : files) {
      String ext = FilenameUtils.getExtension(file.getName());
      if (file.isFile() && (ext.equalsIgnoreCase("jpg") || ext.equalsIgnoreCase("jpeg"))) {
	String path = file.getPath();
	System.out.println("ADDING IMAGE: " + path);

	ImageHeader imageHeader = jpegCodec.decodeHeader(new FileInputStream(path));
	imageHeader.addMetaData("path",path);
	System.out.println(imageHeader);
	aib.addImage(imageHeader, new FileInputStream(path));

      }
    }

    System.out.println("DONE");

    aib.close();
  }

  /*
  @Test
  public void testByteImageIterator() throws IOException {
    AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ, HipiImageFactory.getByteImageFactory());
    ImageDecoder decoder = JpegCodec.getInstance();
    int[] width = {3456, 3072, 2592, 3072, 3456, 4320, 3456, 3456, 1600, 1600, 2048, 1024};
    int[] height = {2304, 2304, 1944, 2304, 2304, 3240, 2304, 2304, 1065, 1065, 1318, 767};
    int[] bands = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
    int count = 0;
    while (aib.hasNext()) {
      System.out.println("VERIFYING IMAGE: " + (count + 1));

      ImageHeader header = aib.next();
      System.out.println(header);

      String sourcePath = header.getMetaData("path");
      System.out.println("sourcePath: " + sourcePath);

      ByteImage image = (ByteImage)aib.getCurrentImage();

      System.out.println(image);

      ByteImage source = (ByteImage)decoder.decodeImage(new FileInputStream(sourcePath), header, HipiImageFactory.getByteImageFactory());

      assertEquals(count + " image header fails", width[count], header.getWidth());
      assertEquals(count + " image header fails", height[count], header.getHeight());
      assertEquals(count + " image fails", width[count], source.getWidth());
      assertEquals(count + " image fails", height[count], source.getHeight());
      assertEquals(count + " image fails", bands[count], source.getNumBands());
      assertEquals(count + " image fails", source, image);
      count++;

    }
    aib.close();
  }

  @Test
  public void testFloatImageIterator() throws IOException {
    AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ, HipiImageFactory.getFloatImageFactory());
    ImageDecoder decoder = JpegCodec.getInstance();
    int[] width = {3456, 3072, 2592, 3072, 3456, 4320, 3456, 3456, 1600, 1600, 2048, 1024};
    int[] height = {2304, 2304, 1944, 2304, 2304, 3240, 2304, 2304, 1065, 1065, 1318, 767};
    int[] bands = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
    int count = 0;
    while (aib.hasNext()) {
      System.out.println("VERIFYING IMAGE: " + (count + 1));

      ImageHeader header = aib.next();
      System.out.println(header);

      String sourcePath = header.getMetaData("path");
      System.out.println("sourcePath: " + sourcePath);

      FloatImage image = (FloatImage)aib.getCurrentImage();

      System.out.println(image);

      FloatImage source = (FloatImage)decoder.decodeImage(new FileInputStream(sourcePath), header, HipiImageFactory.getFloatImageFactory());

      assertEquals(count + " image header fails", width[count], header.getWidth());
      assertEquals(count + " image header fails", height[count], header.getHeight());
      assertEquals(count + " image fails", width[count], source.getWidth());
      assertEquals(count + " image fails", height[count], source.getHeight());
      assertEquals(count + " image fails", bands[count], source.getNumBands());
      assertEquals(count + " image fails", source, image);
      count++;

    }
    aib.close();
  }
  */

}
