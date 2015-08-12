package org.hipi.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.bytedeco.javacpp.opencv_imgcodecs;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.ByteImage;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.RasterImage;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.PpmCodec;
import org.hipi.opencv.OpenCVUtils;
import org.junit.Ignore;
import org.junit.Test;

public class OpenCVUtilsTestCase {
  
  private static final float delta = 0.1f;
  
  private ArrayList<Mat> createTestMatImages() throws IllegalArgumentException, IOException {
    ArrayList<Mat> mats = new ArrayList<Mat>();
    
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    assertNotNull(ppmDecoder);
    
    
    File[] rgbFiles = new File("../testdata/jpeg-rgb").listFiles();
    for (File file : rgbFiles) {
      if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
        String ppmPath = file.getPath();
        Mat mat = opencv_imgcodecs.imread(ppmPath);
        mats.add(mat);
      }
    }
    
    return mats;
  }
  
  private ArrayList<FloatImage> createTestFloatImages() throws IllegalArgumentException, IOException {
    
    ArrayList<FloatImage> floatImages = new ArrayList<FloatImage>();
    
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    assertNotNull(ppmDecoder);
    
    
    File[] rgbFiles = new File("../testdata/jpeg-rgb").listFiles();
    for (File file : rgbFiles) {
      if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
        String ppmPath = file.getPath();
        System.out.println(ppmPath);
        FileInputStream ppmInputStream = new FileInputStream(ppmPath);
        assertNotNull(ppmInputStream);
        HipiImageFactory imageFactory = HipiImageFactory.getFloatImageFactory();
        assertNotNull(imageFactory);
        FloatImage image = (FloatImage)ppmDecoder.decodeHeaderAndImage(ppmInputStream, imageFactory, false);
        floatImages.add(image);
      }
    }
    
    return floatImages;
  }
  
  @Test
  public void testCreateFloatImageFromValidMat() throws IOException {
    int imageCount = 1;
    for(Mat mat : createTestMatImages()) {
      System.out.println("Testing mat number " + imageCount);
      FloatImage image = OpenCVUtils.convertMatToFloatImage(mat);
      
      System.out.println("Testing image dimensions");
      assertEquals("Widths of float image and mat are not equivalent.", mat.cols(), image.getWidth());
      assertEquals("Heights of float image and mat are not equivalent.", mat.rows(), image.getHeight());
      
      System.out.println("Testing image channels");
      assertEquals("Incorrect number of channels in new FloatImage.", mat.channels(), image.getNumBands());
      switch(mat.channels()) {
        case 1:
          assertEquals("Incorrect color space in new FloatImage: " + image.getColorSpace(), HipiColorSpace.LUM, image.getColorSpace());
          break;
        case 3:
          assertEquals("Incorrect color space in new FloatImage: " + image.getColorSpace(), HipiColorSpace.RGB, image.getColorSpace());
          break;
        default:
          fail("Unsupported number of channels: " + mat.channels());
          break;
      }
      
      System.out.println("Testing image contents");
      int elms = (int)(mat.total()*mat.channels());
      float[] imageData = image.getData();
     
      FloatIndexer floatIndexer = (FloatIndexer)mat.createIndexer();
      float [] baselineData = new float[elms];
      for(int i = 0; i < baselineData.length; i++) {
        baselineData[i] = floatIndexer.get(i);
      }
      
      assertEquals("Data arrays have different lengths.", baselineData.length, imageData.length);
      assertArrayEquals("Data arrays are not equivalent.", baselineData, imageData, delta);
      
      imageCount++;    
      System.out.println("");
    }
  }
  
  
  @Test
  public void testCreateMatFromValidFloatImage() throws IOException {
    int imageCount = 1;
    for(FloatImage image : createTestFloatImages()) {
      System.out.println("Testing floatImage number " + imageCount);
      Mat mat;
      
      switch(image.getColorSpace()) {
        case RGB:
          mat = OpenCVUtils.convertFloatImageToMat(image, opencv_core.CV_32FC3);
          break;
        case LUM:
          mat = OpenCVUtils.convertFloatImageToMat(image, opencv_core.CV_32FC1);
          break;
        default:
          throw new IOException("Unsupported HipiColorSpace [" + image.getColorSpace() + "].");
      }
      
      System.out.println("Testing image dimensions");
      assertEquals("Widths of float image and mat are not equivalent.", image.getWidth(), mat.cols());
      assertEquals("Heights of float image and mat are not equivalent.", image.getHeight(), mat.rows());
      
      System.out.println("Testing image channels / data type");
      switch(image.getColorSpace()) {
        case RGB:
          assertEquals("Incorrect data type in new mat.", opencv_core.CV_32FC3, mat.type());
          assertEquals("Incorrect number of channels in new mat.", 3, mat.channels());
          break;
        case LUM:
          assertEquals("Incorrect data type in new mat.", opencv_core.CV_32FC1, mat.type());
          assertEquals("Incorrect number of channels in new mat.", 1, mat.channels());
          break;
        case UNDEFINED:
        default:
          fail("Unsupported color space: " + image.getColorSpace());
          break;
      }
      
      System.out.println("Testing image contents");
      int elms = (int)(mat.total()*mat.channels());
      float[] baselineData = image.getData();
     
      FloatIndexer floatIndexer = (FloatIndexer)mat.createIndexer();
      float [] matData = new float[elms];
      for(int i = 0; i < matData.length; i++) {
        matData[i] = floatIndexer.get(i);
      }
      
      assertEquals("Data arrays have different lengths.", baselineData.length, matData.length);
      assertArrayEquals("Data arrays are not equivalent.", baselineData, matData, delta);
      
      imageCount++;    
      System.out.println("");
    }
  }
}
