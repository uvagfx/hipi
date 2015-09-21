package org.hipi.test;

import static org.junit.Assert.*;

import org.hipi.image.ByteImage;
import org.hipi.image.HipiImage;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.ImageDecoder;
import org.hipi.imagebundle.HipiImageBundle;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HipiImageBundleTestCase {

  public static HipiImageBundle createHibAndOpen(int mode, HipiImageFactory imageFactory) 
  throws IOException {
    HipiImageBundle hib = new HipiImageBundle(new Path(TestUtils.getTmpPath("bundle.hib")),
      new Configuration(), imageFactory);
    if (mode == HipiImageBundle.FILE_MODE_WRITE) {
      hib.openForWrite(true);
    } else {
      hib.openForRead();
    }
    return hib;
  }

  @BeforeClass
  public static void setup() throws IOException {

    System.out.println("HipiImageBundle#setup");

    HipiImageBundle hib = createHibAndOpen(HipiImageBundle.FILE_MODE_WRITE, null);

    JpegCodec jpegCodec = JpegCodec.getInstance();

    File[] files = new File("../testdata/jpeg-rgb").listFiles();
    Arrays.sort(files);

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
    int[] width = {3456, 3072, 2592, 3072, 3456, 640, 500};
    int[] height = {2304, 2304, 1944, 2304, 2304, 480, 375};
    int[] bands = {3, 3, 3, 3, 3, 3, 3};
    
    for (int iter=0; iter<2; iter++) {
      
      HipiImageBundle hib = createHibAndOpen(HipiImageBundle.FILE_MODE_READ, 
        (iter == 0 ? HipiImageFactory.getByteImageFactory() : 
          HipiImageFactory.getFloatImageFactory()));
      
      int count = 0;
      while (hib.next()) {

	System.out.println("VERIFYING IMAGE: " + count);
	
	HipiImageHeader header = hib.currentHeader();
	System.out.println(header);
	
	String sourcePath = header.getMetaData("path");
	System.out.println("sourcePath: " + sourcePath);
	
	HipiImage image = hib.currentImage();

	System.out.println(image);
	
	HipiImage source = (HipiImage)decoder.decodeImage(new FileInputStream(sourcePath), 
    header, (iter == 0 ? HipiImageFactory.getByteImageFactory() : 
      HipiImageFactory.getFloatImageFactory()), true);
	
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

  // Skip test because hard-coded byte offsets may not match due to differences in image encodings
  // (i.e. different versions of ImageIO plugins will produce different compressed byte streams)
  @Ignore
  @Test
  public void testOffsets() throws IOException {
    System.out.println("testOffsets");
    HipiImageBundle hib = (HipiImageBundle)createHibAndOpen(HipiImageBundle.FILE_MODE_READ, null);
    Long trueOffsets[] = {2175103l, 6823640l, 9309588l, 12349470l, 14445907l, 14574029l, 14700480l};
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

    HipiImageBundle hib1 = new HipiImageBundle(new Path(TestUtils.getTmpPath("bundle1.hib")), conf);
    hib1.openForWrite(true);
    hib1.addImage(new FileInputStream("../testdata/jpeg-rgb/01.JPEG"), HipiImageFormat.JPEG);
    hib1.addImage(new FileInputStream("../testdata/jpeg-rgb/02.JPG"), HipiImageFormat.JPEG);
    hib1.close();

    HipiImageBundle hib2 = new HipiImageBundle(new Path(TestUtils.getTmpPath("bundle2.hib")), conf);
    hib2.openForWrite(true);
    hib2.addImage(new FileInputStream("../testdata/jpeg-rgb/03.jpg"), HipiImageFormat.JPEG);
    hib2.addImage(new FileInputStream("../testdata/jpeg-rgb/04.jpg"), HipiImageFormat.JPEG);
    hib2.addImage(new FileInputStream("../testdata/jpeg-rgb/cat.jpg"), HipiImageFormat.JPEG);
    hib2.close();

    HipiImageBundle hib1Read = new HipiImageBundle(
      new Path(TestUtils.getTmpPath("bundle1.hib")), conf);
    HipiImageBundle hib2Read = new HipiImageBundle(
      new Path(TestUtils.getTmpPath("bundle2.hib")), conf);
    HipiImageBundle hibMerged = new HipiImageBundle(
      new Path(TestUtils.getTmpPath("merged_bundle.hib")), conf);
    hibMerged.openForWrite(true);
    hibMerged.append(hib1Read);
    hibMerged.append(hib2Read);
    hibMerged.close();
    hib1Read.close();
    hib2Read.close();

    ImageDecoder decoder = JpegCodec.getInstance();
    int[] width = {3456, 3072, 2592, 3072, 500};
    int[] height = {2304, 2304, 1944, 2304, 375};
            int[] bands = {3, 3, 3, 3, 3};
    String[] fnames = {"01.JPEG", "02.JPG", "03.jpg", "04.jpg", "cat.jpg"};

    HipiImageBundle hib = new HipiImageBundle(new Path(TestUtils.getTmpPath("merged_bundle.hib")),
      conf, HipiImageFactory.getByteImageFactory());
    hib.openForRead();

    for (int i=0; i<4; i++)
      {
	System.out.println("VERIFYING IMAGE: " + fnames[i]);

	boolean hasNext = hib.next();

	assertTrue(hasNext);

	HipiImageHeader header = hib.currentHeader();
	System.out.println(header);
	
	HipiImage image = hib.currentImage();
	System.out.println(image);
	
	HipiImage source = (HipiImage)decoder.decodeImage(
    new FileInputStream("../testdata/jpeg-rgb/"+fnames[i]), header, 
    HipiImageFactory.getByteImageFactory(), true);
	
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
