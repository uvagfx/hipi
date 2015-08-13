package org.hipi.opencv;


import static org.hipi.image.PixelArray.TYPE_BYTE;
import static org.hipi.image.PixelArray.TYPE_SHORT;
import static org.hipi.image.PixelArray.TYPE_USHORT;
import static org.hipi.image.PixelArray.TYPE_INT;
import static org.hipi.image.PixelArray.TYPE_FLOAT;
import static org.hipi.image.PixelArray.TYPE_DOUBLE;
import static org.bytedeco.javacpp.opencv_imgproc.CV_RGB2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.CV_GRAY2RGB;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Size;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.javacpp.indexer.Indexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.PixelArray;
import org.hipi.image.RasterImage;
import org.hipi.util.ByteUtils;

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
    
    if(image == null) {
      throw new IllegalArgumentException("Input RasterImage is null");
    }
    if(image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Input RasterImage has invalid dimensions [" + image.getWidth() + "," + image.getHeight() + "]");
    }
    
    int numBands = image.getNumBands();
    int pixelArrayDataType = image.getPixelArray().getDataType();
    int openCVType = generateOpenCVType(pixelArrayDataType, numBands);
    if(openCVType == -1) {
      throw new IllegalArgumentException("Invalid PixelArray data type [" + pixelArrayDataType + "] and / or RasterImage numBands [" + numBands + "]");
    }
    
    Mat mat = new Mat(image.getHeight(), image.getWidth(), openCVType);
    
    byte[] data = image.getPixelArray().getByteArray();
    
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
  
//  public static Mat convertFloatImageToMat(FloatImage inputImage, OpenCVOutputColorSpace openCVOutputColorSpace) throws IOException {
//    
//    //Step 1: convert FloatImage to a Mat of comparable type
//    Mat unconvertedMat;
//    
//    int rows = inputImage.getHeight();
//    int cols = inputImage.getWidth();
//    HipiColorSpace colorSpace = inputImage.getColorSpace();
//    switch(colorSpace) {
//      case RGB:
//        unconvertedMat = new Mat(rows, cols, opencv_core.CV_32FC3);
//        break;
//      case LUM:
//        unconvertedMat = new Mat(rows, cols, opencv_core.CV_32FC1);
//        break;
//      default:
//        throw new IOException("Unsupported HipiColorSpace [" + colorSpace + "].");
//    }
//    
//    ((FloatBuffer) unconvertedMat.createBuffer()).put(inputImage.getData());
//    
//    
//    //Step 2: perform mat conversion (if necessary)
//    
//    switch(openCVOutputColorSpace) {
//      case OPENCV_RGB:
//        if(colorSpace != HipiColorSpace.RGB) { //convert to rgb only if mat isn't already rgb
//          Mat rgbMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC3, new Scalar(0.0));
//          cvtColor(unconvertedMat, rgbMat, CV_GRAY2RGB);
//          return rgbMat;
//        } else {
//          return unconvertedMat;
//        }
//      case OPENCV_GRAY:
//        if(colorSpace != HipiColorSpace.LUM) { //convert to grayscale only if mat isn't already lum
//          Mat grayMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
//          cvtColor(unconvertedMat, grayMat, CV_RGB2GRAY);
//          return grayMat;
//        } else {
//          return unconvertedMat;
//        }
//      default:
//        throw new IOException("Unsupported opencv color space specified for output mat [" + openCVOutputColorSpace + "].");
//    }
//  }
    
}
