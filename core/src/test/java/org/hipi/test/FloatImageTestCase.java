package org.hipi.test;

import static org.junit.Assert.*;

import org.hipi.image.RasterImage;
import org.hipi.image.FloatImage;
import org.hipi.image.ByteImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.io.ImageCodec;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PpmCodec;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.hipi.image.io.ImageEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;

public class FloatImageTestCase {

  @Test
  public void testSanity() throws IOException {
    FloatImage floatImage = new FloatImage();
    assertEquals(floatImage.getType(), HipiImageType.FLOAT);
    assertEquals(floatImage.getData(), null);
    assertFalse(floatImage.equalsWithTolerance(null, 1.0f));
  }
  
  @Test
  public void testFloatImageWritable() throws IOException {
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    assertNotNull(ppmDecoder);

    File[] cmykFiles = new File("../testdata/jpeg-cmyk").listFiles();
    File[] rgbFiles = new File("../testdata/jpeg-rgb").listFiles();
    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);

    for (int iter=0; iter<2; iter++) {
      for (File file : files) {
        if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
          String ppmPath = file.getPath();
          System.out.println(ppmPath);
          FileInputStream ppmInputStream = new FileInputStream(ppmPath);
          assertNotNull(ppmInputStream);
          HipiImageFactory imageFactory = (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory());
          assertNotNull(imageFactory);
          RasterImage image = (RasterImage)ppmDecoder.decodeHeaderAndImage(ppmInputStream, imageFactory, false);
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          image.write(new DataOutputStream(bos));
          ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
          RasterImage newImage = (iter == 0 ? new ByteImage() : new FloatImage());
          newImage.readFields(new DataInputStream(bis));
          assertEquals(ppmPath + " writable test fails", image, newImage);
        }
      }
    }
    
  }  

}
