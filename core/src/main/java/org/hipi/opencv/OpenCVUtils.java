package org.hipi.opencv;

import static org.bytedeco.javacpp.opencv_imgproc.CV_BGR2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.CV_GRAY2BGR;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;

import java.io.IOException;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
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
  
  
  
  public static FloatImage convertMatToFloatImage(Mat mat) throws IOException {
   
    mat.convertTo(mat, opencv_core.CV_32F);
    
    
    FloatImage image; 
    
    int height = mat.rows();
    int width = mat.cols();
    
    int channels = mat.channels();
    int type = mat.type();
    int elms = (int)(mat.total() * mat.channels());
    image = new FloatImage(width, height, channels);
    switch(channels) {
      case 1:
        image.setHeader(new HipiImageHeader(HipiImageFormat.UNDEFINED, HipiColorSpace.LUM,
            width, height, channels, null, null));
        break;
      case 3:
        image.setHeader(new HipiImageHeader(HipiImageFormat.UNDEFINED, HipiColorSpace.RGB,
            width, height, channels, null, null));
        break;
       default:
         throw new IOException("Unsupported Number of channels in input image [" + channels + "].");  
    }
    
    float[] data = new float[elms];
    FloatIndexer floatIndexer = (FloatIndexer)mat.createIndexer();
    for(int i = 0; i < mat.total() * mat.channels(); i++) {
          data[i] = floatIndexer.get(i);
    }
    
    image.setData(data);
    return image;
  }
  
  public static Mat convertFloatImageToMat(FloatImage image, int matType) throws IOException {
    
    //Step 1: convert FloatImage to a Mat of comparable type
    Mat unconvertedMat;
    
    int rows = image.getHeight();
    int cols = image.getWidth();
    HipiColorSpace colorSpace = image.getColorSpace();
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
    
    FloatIndexer floatIndexer = (FloatIndexer)unconvertedMat.createIndexer();
    for(int i = 0; i < unconvertedMat.total() * unconvertedMat.channels(); i++) {
          floatIndexer.put(i, image.getData()[i]);     
    }
    
    //Step 2: perform mat conversion (if necessary)
    //Unable to use switch statement because opencv_core datatype identifiers are not constant

    //will throw exception on conversion if mat is single-channel TODO: confirm this
    if(matType == opencv_core.CV_32FC1) {
      if(unconvertedMat.channels() != 1) {
        Mat singleChannelMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
        cvtColor(unconvertedMat, singleChannelMat, CV_BGR2GRAY);
        return singleChannelMat;
      } else {
        return unconvertedMat;
      }
    } else if(matType == opencv_core.CV_32FC3) {
      if(unconvertedMat.channels() == 1) {
        Mat tripleChannelMat = new Mat(unconvertedMat.rows(), unconvertedMat.cols(), opencv_core.CV_32FC3, new Scalar(0.0));
        cvtColor(unconvertedMat, tripleChannelMat, CV_GRAY2BGR);
        return tripleChannelMat;
      } else {
        return unconvertedMat;
      }
    } else {
      throw new IOException("Unsupported matType [" + matType + "].");
    }
    
  }
    
}
