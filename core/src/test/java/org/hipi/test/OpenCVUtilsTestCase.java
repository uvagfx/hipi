package org.hipi.test;

import static org.junit.Assert.*;

import org.hipi.image.ByteImage;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.PixelArray;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.PpmCodec;
import org.hipi.opencv.OpenCVUtils;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;

public class OpenCVUtilsTestCase {
  
  private float delta = 0.05f;
  
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
  
  private ArrayList<ByteImage> createTestByteImages() throws IllegalArgumentException, IOException {
    
    ArrayList<ByteImage> byteImages = new ArrayList<ByteImage>();
    
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    assertNotNull(ppmDecoder);
    
    
    File[] rgbFiles = new File("../testdata/jpeg-rgb").listFiles();
    for (File file : rgbFiles) {
      if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
        String ppmPath = file.getPath();
        System.out.println(ppmPath);
        FileInputStream ppmInputStream = new FileInputStream(ppmPath);
        assertNotNull(ppmInputStream);
        HipiImageFactory imageFactory = HipiImageFactory.getByteImageFactory();
        assertNotNull(imageFactory);
        ByteImage image = (ByteImage)ppmDecoder.decodeHeaderAndImage(ppmInputStream, imageFactory, false);
        byteImages.add(image);
      }
    }
    
    return byteImages;
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
    OpenCVUtils.convertRasterImageToMat(image);
  }
  
  @Test
  public void testConvertRasterImageToMatWithFloatImages() throws IllegalArgumentException, IOException {
    
    for(FloatImage image : createTestFloatImages()) {
      
      Mat mat = OpenCVUtils.convertRasterImageToMat(image);
      
      assertEquals("Width of converted mat is incorrect.", image.getWidth(), mat.cols());
      assertEquals("Height of converted mat is incorrect.", image.getHeight(), mat.rows());
      
      assertEquals("opencv type of converted mat is incorrect", OpenCVUtils.generateOpenCVType(image.getPixelArray().getDataType(), image.getNumBands()), mat.type());
      
      float[] benchmark = image.getData();
      float[] convertedData = new float[benchmark.length];
      ((FloatBuffer)mat.createBuffer()).get(convertedData);      
      assertArrayEquals("contents of converted mat are incorrect", image.getData(), convertedData, delta);
    }
    
   
  }
  
  @Test
  public void testConvertRasterImageToMatWithByteImages() throws IllegalArgumentException, IOException {
    for(ByteImage image : createTestByteImages()) {
      
      Mat mat = OpenCVUtils.convertRasterImageToMat(image);
      
      assertEquals("width of converted mat is incorrect", image.getWidth(), mat.cols());
      assertEquals("height of converted mat is incorrect", image.getHeight(), mat.rows());
      
      assertEquals("opencv type of converted mat is incorrect", OpenCVUtils.generateOpenCVType(image.getPixelArray().getDataType(), image.getNumBands()), mat.type());
      
      byte[] benchmark = image.getData();
      byte[] convertedData = new byte[benchmark.length];
      ((ByteBuffer)mat.createBuffer()).get(convertedData);      
      assertArrayEquals("contents of converted mat are incorrect", image.getData(), convertedData);
    }
  }
}
