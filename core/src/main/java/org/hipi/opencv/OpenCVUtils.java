package org.hipi.opencv;

import org.hipi.image.RasterImage;
import org.hipi.util.ByteUtils;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

public class OpenCVUtils {
  
  private static int[][] openCVTypeLookupTable = new int[][] {{opencv_core.CV_8UC1, opencv_core.CV_8UC2, opencv_core.CV_8UC3, opencv_core.CV_8UC4},
                                                              {opencv_core.CV_16UC1, opencv_core.CV_16UC2, opencv_core.CV_16UC3, opencv_core.CV_16UC4},
                                                              {opencv_core.CV_16SC1, opencv_core.CV_16SC2, opencv_core.CV_16SC3, opencv_core.CV_16SC4},
                                                              {opencv_core.CV_32SC1, opencv_core.CV_32SC2, opencv_core.CV_32SC3, opencv_core.CV_32SC4},
                                                              {opencv_core.CV_32FC1, opencv_core.CV_32FC2, opencv_core.CV_32FC3, opencv_core.CV_32FC4},
                                                              {opencv_core.CV_64FC1, opencv_core.CV_64FC2, opencv_core.CV_64FC3, opencv_core.CV_64FC4}};
  
  public static int generateOpenCVType(int pixelArrayDataType, int numBands) {
    
    int depthIndex = pixelArrayDataType;
    int channelIndex = numBands - 1;
    
    if(depthIndex < 0 || depthIndex >= openCVTypeLookupTable.length) {
      return -1;
    }
    if(channelIndex < 0 || channelIndex >= openCVTypeLookupTable[0].length) {
      return -1;
    }
    
    return openCVTypeLookupTable[depthIndex][channelIndex];
  }
  
  public static Mat convertRasterImageToMat(RasterImage image) throws IllegalArgumentException {
    
    // Check for invalid input
    if(image == null) {
      throw new IllegalArgumentException("Input RasterImage is null.");
    }
    if(image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Input RasterImage has invalid dimensions [" + image.getWidth() + "," + image.getHeight() + "]");
    }
    
    // Generate opencv data type based on input pixel array data type / number of bands
    int numBands = image.getNumBands();
    int pixelArrayDataType = image.getPixelArray().getDataType();
    int openCVType = generateOpenCVType(pixelArrayDataType, numBands);
    if(openCVType == -1) {
      throw new IllegalArgumentException("Invalid PixelArray data type [" + pixelArrayDataType + "] and / or RasterImage numBands [" + numBands + "]");
    }
    
    // Create output mat
    Mat mat = new Mat(image.getHeight(), image.getWidth(), openCVType);
    
    // Access raster image data
    byte[] data = image.getPixelArray().getByteArray();
    
    // Transfer data into mat
    switch(opencv_core.CV_MAT_DEPTH(mat.type())) {
      case opencv_core.CV_8U:
      case opencv_core.CV_8S:
        ((ByteBuffer)mat.createBuffer()).put(data);
        break;
      case opencv_core.CV_16U:
      case opencv_core.CV_16S:      
        ((ShortBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToShortArray(data));
        break;
      case opencv_core.CV_32S:
        ((IntBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToIntArray(data));
        break;
      case opencv_core.CV_32F:
        ((FloatBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToFloatArray(data));
        break;
      case opencv_core.CV_64F:
        ((DoubleBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToDoubleArray(data));
        break;
      default:
        throw new IllegalArgumentException("Unsupported matrix depth [" + opencv_core.CV_MAT_DEPTH(mat.type()) + "].");
    }
    
    return mat;
  } 
}
