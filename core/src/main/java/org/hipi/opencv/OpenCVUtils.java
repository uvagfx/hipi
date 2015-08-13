package org.hipi.opencv;

import static org.bytedeco.javacpp.opencv_imgproc.CV_RGB2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.CV_GRAY2RGB;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;

import java.io.IOException;
import java.nio.FloatBuffer;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.javacpp.indexer.Indexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.util.ByteUtils;

public class OpenCVUtils {
  
  public enum OpenCVOutputColorSpace {
    OPENCV_RGB, OPENCV_GRAY
  }
  
  
  public static FloatImage convertMatToFloatImage(Mat inputMat, HipiColorSpace outputHipiColorSpace, HipiImageFormat outputHipiImageFormat) throws IOException {
   
    //ensure that base mat data type is compatible with FloatImage
    inputMat.convertTo(inputMat, opencv_core.CV_32F);
    
    //Step 1: convert input mat (if necessary) to meet hipiOutputColorSpace specification
    Mat convertedMat;
    switch(outputHipiColorSpace) {
      case RGB:
        if(inputMat.channels() != 3) {
          convertedMat = new Mat(inputMat.rows(), inputMat.cols(), opencv_core.CV_32FC3);
          cvtColor(inputMat, convertedMat, CV_GRAY2RGB);
        } else {
          convertedMat = inputMat;
        }
        break;
      case LUM:
        if(inputMat.channels() != 1) {
          convertedMat = new Mat(inputMat.rows(), inputMat.cols(), opencv_core.CV_32FC1);
          cvtColor(inputMat, convertedMat, CV_RGB2GRAY);
        } else {
          convertedMat = inputMat;
        }
        break;
     default:
       throw new IOException("Unsupported hipi color space specified for output FloatImage [" + outputHipiColorSpace + "].");
    }
    
    
    //Step 2: build FloatImage from converted mat and input parameters which help to define the header.
    int height = convertedMat.rows();
    int width = convertedMat.cols();   
    int channels = convertedMat.channels();
    FloatImage unconvertedImage = new FloatImage(width, height, channels);
    unconvertedImage.setHeader(new HipiImageHeader(outputHipiImageFormat, outputHipiColorSpace, width, height, channels, null, null));

    float [] data = new float[(int) (convertedMat.total() * convertedMat.channels())];
    ((FloatBuffer) convertedMat.createBuffer()).get(data);
    unconvertedImage.setData(data);
    return unconvertedImage;
  }
  
  public static Mat convertFloatImageToMat(FloatImage inputImage, OpenCVOutputColorSpace openCVOutputColorSpace) throws IOException {
    
    //Step 1: convert FloatImage to a Mat of comparable type
    Mat unconvertedMat;
    
    int rows = inputImage.getHeight();
    int cols = inputImage.getWidth();
    HipiColorSpace colorSpace = inputImage.getColorSpace();
    switch(colorSpace) {
      case RGB:
        unconvertedMat = new Mat(rows, cols, opencv_core.CV_32FC3);
        break;
      case LUM:
        unconvertedMat = new Mat(rows, cols, opencv_core.CV_32FC1);
        break;
      default:
        throw new IOException("Unsupported HipiColorSpace [" + colorSpace + "].");
    }
    
    ((FloatBuffer) unconvertedMat.createBuffer()).put(inputImage.getData());
    
    
    //Step 2: perform mat conversion (if necessary)
    
    switch(openCVOutputColorSpace) {
      case OPENCV_RGB:
        if(colorSpace != HipiColorSpace.RGB) { //convert to rgb only if mat isn't already rgb
          Mat rgbMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC3, new Scalar(0.0));
          cvtColor(unconvertedMat, rgbMat, CV_GRAY2RGB);
          return rgbMat;
        } else {
          return unconvertedMat;
        }
      case OPENCV_GRAY:
        if(colorSpace != HipiColorSpace.LUM) { //convert to grayscale only if mat isn't already lum
          Mat grayMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
          cvtColor(unconvertedMat, grayMat, CV_RGB2GRAY);
          return grayMat;
        } else {
          return unconvertedMat;
        }
      default:
        throw new IOException("Unsupported opencv color space specified for output mat [" + openCVOutputColorSpace + "].");
    }
  }
    
}
