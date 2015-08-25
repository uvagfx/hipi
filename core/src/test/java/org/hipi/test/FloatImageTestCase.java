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
  
  @Test
  public void testImageConvolution() throws IllegalArgumentException, IOException {
    String[] filenames = {"fall", "stonehendge", "panda", "jpg", "giraffe"};
    String directory = "../testdata/convolution/";
    ImageDecoder jpgCodec = JpegCodec.getInstance();
    ImageDecoder ppmCodec = PpmCodec.getInstance();
    HipiImageFactory imagegFactory = HipiImageFactory.getFloatImageFactory();
    FloatImage filter = FloatImage.gaussianFilter(2);
    
    for (int i = 0; i < filenames.length; i++) {
      // Read in a test image
      String inputPath = directory + filenames[i] + ".jpg";
      FileInputStream inputStream = new FileInputStream(inputPath);
      FloatImage inputImg = (FloatImage) jpgCodec.decodeHeaderAndImage(inputStream, imagegFactory, false);
      
      // Convolve (CPU)
      FloatImage cpuImg = new FloatImage(inputImg.getWidth(), inputImg.getHeight(), inputImg.getNumBands(), HipiImageFormat.PPM, HipiColorSpace.RGB);
      inputImg.convolution(filter, cpuImg);
      
      // Read in the truth image
      String truthPath = directory + filenames[i] + ".ppm";
      inputStream = new FileInputStream(truthPath);
      FloatImage truthImg = (FloatImage) ppmCodec.decodeHeaderAndImage(inputStream, imagegFactory, false);
      
      assertTrue("Failed image: " + filenames[i], cpuImg.equalsWithTolerance(truthImg, 5.0f/255.0f));
    }
    
  }

}
