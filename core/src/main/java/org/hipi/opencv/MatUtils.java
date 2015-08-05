package org.hipi.opencv;

import java.io.IOException;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.util.ByteUtils;

public class MatUtils {
  
  public static FloatImage convertMatToFloatImage(Mat mat) throws IOException {
    
    FloatImage image; 
    
    int rows = mat.rows();
    int cols = mat.cols();
    
    int channels = mat.channels();
    int elms = (int)(mat.total() * mat.channels());
    switch(channels) {
      case 1:
        image = new FloatImage(rows, cols, 1);
        break;
      case 3:
        image = new FloatImage(rows, cols, 3);
        break;
       default:
         throw new IOException("Unsupported Number of channels in input image [" + channels + "].");
        
    }
    
    byte[] data = new byte[elms * 4];
    mat.data().get(data);
//    image.setData(data);
    return image;
    
    
  }
  
  public static Mat convertFloatImageToMat(FloatImage image) throws IOException {
    
    Mat mat;
    
    int rows = image.getHeight();
    int cols = image.getWidth();
    HipiColorSpace colorSpace = image.getColorSpace();
    switch(colorSpace) {
      case RGB:
        mat = new Mat(rows, cols, opencv_core.CV_32FC3);
        break;
      case LUM:
        mat = new Mat(rows, cols, opencv_core.CV_32FC1);
        break;
      default:
        throw new IOException("Unsupported HipiColorSpace [" + colorSpace + "].");
    }
    mat.data(new BytePointer(ByteUtils.floatArrayToByteArray(image.getData())));
    return mat;
  }
    
}
