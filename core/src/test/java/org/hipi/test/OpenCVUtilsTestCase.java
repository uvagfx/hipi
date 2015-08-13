package org.hipi.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgcodecs;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.ByteImage;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.PixelArray;
import org.hipi.image.RasterImage;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.PpmCodec;
import org.hipi.opencv.OpenCVUtils;
import org.hipi.util.ByteUtils;
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
  
  private FloatImage createTestFloatImage() throws IllegalArgumentException, IOException {
    
    
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    assertNotNull(ppmDecoder);
    
    
    File file = new File("../testdata/jpeg-rgb").listFiles()[0];
    if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
      String ppmPath = file.getPath();
      System.out.println(ppmPath);
      FileInputStream ppmInputStream = new FileInputStream(ppmPath);
      assertNotNull(ppmInputStream);
      HipiImageFactory imageFactory = HipiImageFactory.getFloatImageFactory();
      assertNotNull(imageFactory);
      FloatImage image = (FloatImage)ppmDecoder.decodeHeaderAndImage(ppmInputStream, imageFactory, false);
      return image;
    }
    return null;
  }
  
  @Test
  public void testGenerateOpenCVTypeInvalidPixelArrayType() {
    assertEquals("Invalid lookup was not caught.", -1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_UNDEFINED, 1));
  }
  
  @Test
  public void testGenerateOpenCVTypeInvalidChannels() {
    assertEquals("Invalid lookup was not caught.", -1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_BYTE, -1));
  }
  
  @Test public void testGenerateOpenCVTypeIntegrityOfLookupTable() {
    assertEquals("Lookup of opencv_core.CV_8UC1 failed.", opencv_core.CV_8UC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_BYTE, 1));
    assertEquals("Lookup of opencv_core.CV_8UC2 failed.", opencv_core.CV_8UC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_BYTE, 2));
    assertEquals("Lookup of opencv_core.CV_8UC3 failed.", opencv_core.CV_8UC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_BYTE, 3));
    assertEquals("Lookup of opencv_core.CV_8UC4 failed.", opencv_core.CV_8UC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_BYTE, 4));
    
    assertEquals("Lookup of opencv_core.CV_16UC1 failed.", opencv_core.CV_16UC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_USHORT, 1));
    assertEquals("Lookup of opencv_core.CV_16UC2 failed.", opencv_core.CV_16UC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_USHORT, 2));
    assertEquals("Lookup of opencv_core.CV_16UC3 failed.", opencv_core.CV_16UC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_USHORT, 3));
    assertEquals("Lookup of opencv_core.CV_16UC4 failed.", opencv_core.CV_16UC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_USHORT, 4));
    
    assertEquals("Lookup of opencv_core.CV_16SC1 failed.", opencv_core.CV_16SC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_SHORT, 1));
    assertEquals("Lookup of opencv_core.CV_16SC2 failed.", opencv_core.CV_16SC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_SHORT, 2));
    assertEquals("Lookup of opencv_core.CV_16SC3 failed.", opencv_core.CV_16SC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_SHORT, 3));
    assertEquals("Lookup of opencv_core.CV_16SC4 failed.", opencv_core.CV_16SC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_SHORT, 4));
    
    assertEquals("Lookup of opencv_core.CV_32SC1 failed.", opencv_core.CV_32SC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_INT, 1));
    assertEquals("Lookup of opencv_core.CV_32SC2 failed.", opencv_core.CV_32SC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_INT, 2));
    assertEquals("Lookup of opencv_core.CV_32SC3 failed.", opencv_core.CV_32SC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_INT, 3));
    assertEquals("Lookup of opencv_core.CV_32SC4 failed.", opencv_core.CV_32SC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_INT, 4));
    
    assertEquals("Lookup of opencv_core.CV_32FC1 failed.", opencv_core.CV_32FC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_FLOAT, 1));
    assertEquals("Lookup of opencv_core.CV_32FC2 failed.", opencv_core.CV_32FC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_FLOAT, 2));
    assertEquals("Lookup of opencv_core.CV_32FC3 failed.", opencv_core.CV_32FC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_FLOAT, 3));
    assertEquals("Lookup of opencv_core.CV_32FC4 failed.", opencv_core.CV_32FC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_FLOAT, 4));
    
    assertEquals("Lookup of opencv_core.CV_64FC1 failed.", opencv_core.CV_64FC1, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_DOUBLE, 1));
    assertEquals("Lookup of opencv_core.CV_64FC2 failed.", opencv_core.CV_64FC2, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_DOUBLE, 2));
    assertEquals("Lookup of opencv_core.CV_64FC3 failed.", opencv_core.CV_64FC3, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_DOUBLE, 3));
    assertEquals("Lookup of opencv_core.CV_64FC4 failed.", opencv_core.CV_64FC4, OpenCVUtils.generateOpenCVType(PixelArray.TYPE_DOUBLE, 4));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConvertRasterImageToMatNullInput() {
    OpenCVUtils.convertRasterImageToMat(null);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConvertRasterImageToMatInvalidSize() throws IllegalArgumentException, IOException {
    FloatImage image = createTestFloatImage();
    image.setHeader(new HipiImageHeader(HipiImageFormat.JPEG, HipiColorSpace.RGB, 0, 0, 0, null, null));
    OpenCVUtils.convertRasterImageToMat(new FloatImage());
  }
  
//  @Test
//  public void testCreateFloatImageFromValidMat() throws IOException {
//    int imageCount = 1;
//    for(Mat mat : createTestMatImages()) {
// 
//      System.out.println("Testing mat number " + imageCount);
//      FloatImage image = OpenCVUtils.convertMatToFloatImage(mat, HipiColorSpace.RGB, HipiImageFormat.UNDEFINED);
//      
//      System.out.println("Testing image dimensions");
//      assertEquals("Widths of float image and mat are not equivalent.", mat.cols(), image.getWidth());
//      assertEquals("Heights of float image and mat are not equivalent.", mat.rows(), image.getHeight());
//      
//      System.out.println("Testing image channels");
//      assertEquals("Incorrect number of channels in new FloatImage.", mat.channels(), image.getNumBands());
//      switch(mat.channels()) {
//        case 1:
//          assertEquals("Incorrect color space in new FloatImage: " + image.getColorSpace(), HipiColorSpace.LUM, image.getColorSpace());
//          break;
//        case 3:
//          assertEquals("Incorrect color space in new FloatImage: " + image.getColorSpace(), HipiColorSpace.RGB, image.getColorSpace());
//          break;
//        default:
//          fail("Unsupported number of channels: " + mat.channels());
//          break;
//      }
//      
//      System.out.println("Testing image contents");
//      float[] imageData = image.getData();
//      
//      float[] baselineDataAsFloatArray = new float[imageData.length];
//      ((FloatBuffer)mat.createBuffer()).get(baselineDataAsFloatArray);
//      
//      assertEquals("Data arrays have different lengths.", baselineDataAsFloatArray.length, imageData.length);
//      assertArrayEquals("Data arrays are not equivalent.", baselineDataAsFloatArray, imageData, delta);
//      
//      imageCount++;    
//      System.out.println("");
//    }
//  }
//  
  
//  @Test
//  public void testCreateMatFromValidFloatImage() throws IOException {
//    int imageCount = 1;
//    for(FloatImage image : createTestFloatImages()) {
//      System.out.println("Testing floatImage number " + imageCount);
//      Mat mat;
//      
//      switch(image.getColorSpace()) {
//        case RGB:
//          mat = OpenCVUtils.convertFloatImageToMat(image, OpenCVUtils.OpenCVOutputColorSpace.OPENCV_RGB);
//          break;
//        case LUM:
//          mat = OpenCVUtils.convertFloatImageToMat(image, OpenCVUtils.OpenCVOutputColorSpace.OPENCV_GRAY);
//          break;
//        default:
//          throw new IOException("Unsupported HipiColorSpace [" + image.getColorSpace() + "].");
//      }
//      
//      System.out.println("Testing image dimensions");
//      assertEquals("Widths of float image and mat are not equivalent.", image.getWidth(), mat.cols());
//      assertEquals("Heights of float image and mat are not equivalent.", image.getHeight(), mat.rows());
//      
//      System.out.println("Testing image channels / data type");
//      switch(image.getColorSpace()) {
//        case RGB:
//          assertEquals("Incorrect data type in new mat.", opencv_core.CV_32FC3, mat.type());
//          assertEquals("Incorrect number of channels in new mat.", 3, mat.channels());
//          break;
//        case LUM:
//          assertEquals("Incorrect data type in new mat.", opencv_core.CV_32FC1, mat.type());
//          assertEquals("Incorrect number of channels in new mat.", 1, mat.channels());
//          break;
//        case UNDEFINED:
//        default:
//          fail("Unsupported color space: " + image.getColorSpace());
//          break;
//      }
//      
//      System.out.println("Testing image contents");
//      float[] baselineData = image.getData();
//     
//      float[] matDataAsFloatArray = new float[baselineData.length];
//      ((FloatBuffer)mat.createBuffer()).get(matDataAsFloatArray);
//      
//      assertEquals("Data arrays have different lengths.", baselineData.length, matDataAsFloatArray.length);
//      assertArrayEquals("Data arrays are not equivalent.", baselineData, matDataAsFloatArray, delta);
//      
//      imageCount++;    
//      System.out.println("");
//    }
//  }
}
