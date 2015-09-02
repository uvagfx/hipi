package org.hipi.opencv;

import org.hipi.image.PixelArray;
import org.hipi.image.RasterImage;
import org.hipi.util.ByteUtils;
import org.apache.hadoop.mapreduce.RecordReader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

/**
 * Various static helper methods which facilitate conversion between HIPI and OpenCV image 
 * representations.
 */
public class OpenCVUtils {

  private static int[][] openCVTypeLUT = new int[][] 
      {{opencv_core.CV_8UC1, opencv_core.CV_8UC2, opencv_core.CV_8UC3, opencv_core.CV_8UC4},     
       {opencv_core.CV_16UC1, opencv_core.CV_16UC2, opencv_core.CV_16UC3, opencv_core.CV_16UC4},
       {opencv_core.CV_16SC1, opencv_core.CV_16SC2, opencv_core.CV_16SC3, opencv_core.CV_16SC4},
       {opencv_core.CV_32SC1, opencv_core.CV_32SC2, opencv_core.CV_32SC3, opencv_core.CV_32SC4},
       {opencv_core.CV_32FC1, opencv_core.CV_32FC2, opencv_core.CV_32FC3, opencv_core.CV_32FC4},
       {opencv_core.CV_64FC1, opencv_core.CV_64FC2, opencv_core.CV_64FC3, opencv_core.CV_64FC4}};
  
  /**
   * Returns the OpenCV data type which is associated with a particular {@link PixelArray} data type 
   * and number of bands.
   * 
   * @return integer representation of OpenCV data type
   */
  public static int generateOpenCVType(int pixelArrayDataType, int numBands) {
    
    int depthIndex = -1;
    switch(pixelArrayDataType) {
      case PixelArray.TYPE_BYTE:
        depthIndex = 0;
        break;
      case PixelArray.TYPE_USHORT:
        depthIndex = 1;
        break;
      case PixelArray.TYPE_SHORT:
        depthIndex = 2;
        break;
      case PixelArray.TYPE_INT:
        depthIndex = 3;
        break;
      case PixelArray.TYPE_FLOAT:
        depthIndex = 4;
        break;
      case PixelArray.TYPE_DOUBLE:
        depthIndex = 5;
        break;
      default:
        break;
    }
    
    int channelIndex = numBands - 1;
    
    if(depthIndex < 0 || depthIndex >= openCVTypeLUT.length) {
      return -1;
    }
    if(channelIndex < 0 || channelIndex >= openCVTypeLUT[0].length) {
      return -1;
    }
    
    return openCVTypeLUT[depthIndex][channelIndex];
  }
  
  /**
   * Converts an input {@link RasterImage} into an {@link Mat}. 
   * 
   * @return {@link Mat} of same data type and dimensions as input image
   * @throws IllegalArgumentException
   */
  public static Mat convertRasterImageToMat(RasterImage image) throws IllegalArgumentException {
    
    // Check for invalid input
    if(image == null) {
      throw new IllegalArgumentException("Input RasterImage is null.");
    }
    if(image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Input RasterImage has invalid dimensions: "
          + "[" + image.getWidth() + "," + image.getHeight() + "]");
    }
    if(image.getNumBands() <= 0 || image.getNumBands() > 4) {
      throw new IllegalArgumentException("Input RasterImage has invalid number of bandsY: "
          + "[" + image.getNumBands() + "]");
    }
    
    // Generate opencv data type based on input pixel array data type / number of bands
    int pixelArrayDataType = image.getPixelArray().getDataType();
    int numBands = image.getNumBands();
    int openCVType = generateOpenCVType(pixelArrayDataType, numBands);
    if(openCVType == -1) {
      throw new IllegalArgumentException("Invalid PixelArray data type: "
          + "[" + pixelArrayDataType + "] and / or RasterImage numBands: [" + numBands + "]");
    }
    
    // Create output mat
    Mat mat = new Mat(image.getHeight(), image.getWidth(), openCVType);
    
    // Access raster image data
    byte[] data = image.getPixelArray().getByteArray();
    
    // Transfer data into mat
    int depth = opencv_core.CV_MAT_DEPTH(mat.type());
    switch(depth) {
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
        throw new IllegalArgumentException("Unsupported matrix depth [" + depth + "].");
    }
    
    return mat;
  } 
}
