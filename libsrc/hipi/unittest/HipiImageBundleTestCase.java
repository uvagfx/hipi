package hipi.unittest;

import static org.junit.Assert.*;

import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.image.HipiImage;
import hipi.image.ByteImage;
import hipi.image.FloatImage;
import hipi.image.HipiImageFactory;
import hipi.image.io.JpegCodec;
import hipi.image.io.ImageDecoder;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class HipiImageBundleTestCase {

  public static HipiImageBundle createHibAndOpen(int mode, HipiImageFactory imageFactory) throws IOException {
    HipiImageBundle hib = new HipiImageBundle(imageFactory, new Path("/tmp/bundle.hib"), new Configuration());
    hib.open(mode, true);
    return hib;
  }

  @BeforeClass
  public static void setup() throws IOException {

    System.out.println("HipiImageBundle#setup");

    HipiImageBundle hib = createHibAndOpen(AbstractImageBundle.FILE_MODE_WRITE, null);

    JpegCodec jpegCodec = JpegCodec.getInstance();

    File[] files = new File("./testimages/jpeg-rgb").listFiles();

    for (File file : files) {
      String ext = FilenameUtils.getExtension(file.getName());
      if (file.isFile() && (ext.equalsIgnoreCase("jpg") || ext.equalsIgnoreCase("jpeg"))) {
	String path = file.getPath();
	System.out.println("ADDING IMAGE: " + path);

	HipiImageHeader imageHeader = jpegCodec.decodeHeader(new FileInputStream(path));
	imageHeader.addMetaData("path",path);
	System.out.println(imageHeader);
	hib.addImage(imageHeader, new FileInputStream(path));

      }
    }

    System.out.println("DONE");

    hib.close();
  }  

  @Test
  public void testIterator() throws IOException {

    ImageDecoder decoder = JpegCodec.getInstance();
    int[] width = {3456, 3072, 2592, 3072, 3456, 4320, 3456, 3456, 1600, 1600, 2048, 1024};
    int[] height = {2304, 2304, 1944, 2304, 2304, 3240, 2304, 2304, 1065, 1065, 1318, 767};
    int[] bands = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
    
    for (int iter=0; iter<2; iter++) {
      
      HipiImageBundle hib = createHibAndOpen(AbstractImageBundle.FILE_MODE_READ, (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory()));
      
      int count = 0;
      while (hib.hasNext()) {

	System.out.println("VERIFYING IMAGE: " + (count + 1));
	
	HipiImageHeader header = hib.next();
	System.out.println(header);
	
	String sourcePath = header.getMetaData("path");
	System.out.println("sourcePath: " + sourcePath);
	
	HipiImage image = hib.getCurrentImage();

	System.out.println(image);
	
	HipiImage source = (HipiImage)decoder.decodeImage(new FileInputStream(sourcePath), header, (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory()), true);
	
	assertEquals(count + " image header fails", width[count], header.getWidth());
	assertEquals(count + " image header fails", height[count], header.getHeight());
	assertEquals(count + " image fails", width[count], source.getWidth());
	assertEquals(count + " image fails", height[count], source.getHeight());
	assertEquals(count + " image fails", bands[count], source.getNumBands());
	assertEquals(count + " image fails", source, image);
	count++;
	
      }
      hib.close();
    }

  }

  @Test
  public void testOffsets() throws IOException {
    System.out.println("testOffsets");
    HipiImageBundle hib = (HipiImageBundle)createHibAndOpen(AbstractImageBundle.FILE_MODE_READ, null);
    Long trueOffsets[] = {2175104l, 6823642l, 9309591l, 12349474l, 14445912l, 14574035l};
    List<Long> offsets = hib.readAllOffsets();
    System.out.println(offsets);
    assertEquals(offsets.size(), trueOffsets.length);
    for (int i=0; i<trueOffsets.length; i++) {
      System.out.println(offsets.get(i));
      assertEquals(trueOffsets[i], offsets.get(i));
    }
  }

  @Test
  public void testAppend() throws IOException {
    System.out.println("testAppend");

    // create image bundles
    Configuration conf = new Configuration();

    HipiImageBundle hib1 = new HipiImageBundle(null, new Path("/tmp/bundle1.hib"), conf);
    hib1.open(AbstractImageBundle.FILE_MODE_WRITE, true);
    hib1.addImage(new FileInputStream("testimages/jpeg-rgb/01.JPEG"), HipiImageFormat.JPEG);
    hib1.addImage(new FileInputStream("testimages/jpeg-rgb/02.JPG"), HipiImageFormat.JPEG);
    hib1.close();

    HipiImageBundle hib2 = new HipiImageBundle(null, new Path("/tmp/bundle2.hib"), conf);
    hib2.open(AbstractImageBundle.FILE_MODE_WRITE, true);
    hib2.addImage(new FileInputStream("testimages/jpeg-rgb/03.jpg"), HipiImageFormat.JPEG);
    hib2.addImage(new FileInputStream("testimages/jpeg-rgb/04.jpg"), HipiImageFormat.JPEG);
    hib2.close();

    HipiImageBundle hib1Read = new HipiImageBundle(null, new Path("/tmp/bundle1.hib"), conf);
    HipiImageBundle hib2Read = new HipiImageBundle(null, new Path("/tmp/bundle2.hib"), conf);

    HipiImageBundle hibMerged = new HipiImageBundle(null, new Path("/tmp/merged_bundle.hib"), conf);
    hibMerged.open(HipiImageBundle.FILE_MODE_WRITE, true);
    hibMerged.append(hib1Read);
    hibMerged.append(hib2Read);
    hibMerged.close();
    hib1Read.close();
    hib2Read.close();

    ImageDecoder decoder = JpegCodec.getInstance();
    int[] width = {3456, 3072, 2592, 3072, 3456, 4320, 3456, 3456, 1600, 1600, 2048, 1024};
    int[] height = {2304, 2304, 1944, 2304, 2304, 3240, 2304, 2304, 1065, 1065, 1318, 767};
    int[] bands = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
    String[] fnames = {"01.JPEG", "02.JPG", "03.jpg", "04.jpg"};

    HipiImageBundle hib = new HipiImageBundle(HipiImageFactory.getByteImageFactory(), new Path("/tmp/merged_bundle.hib"), conf);
    hib.open(HipiImageBundle.FILE_MODE_READ, false);

    for (int i=0; i<4; i++)
      {
	System.out.println("VERIFYING IMAGE: " + fnames[i]);

	assertTrue(hib.hasNext());

	HipiImageHeader header = hib.next();
	System.out.println(header);
	
	HipiImage image = hib.getCurrentImage();
	System.out.println(image);
	
	HipiImage source = (HipiImage)decoder.decodeImage(new FileInputStream("testimages/jpeg-rgb/"+fnames[i]), header, HipiImageFactory.getByteImageFactory(), true);
	
	assertEquals(width[i], header.getWidth());
	assertEquals(height[i], header.getHeight());
	assertEquals(width[i], source.getWidth());
	assertEquals(height[i], source.getHeight());
	assertEquals(bands[i], source.getNumBands());
	assertEquals(source, image);
      }

    hib.close();
    

    System.out.println("DONE");
  }

  /*
  @Test
  public void testHibImageFactoryConditions() {
  }
  */

}
