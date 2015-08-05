package org.hipi.opencv;

import org.apache.hadoop.io.Writable;
import org.hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;

public class OpenCVMatWritable implements Writable {

  private Mat mat = null;

  public OpenCVMatWritable() {
    mat = new Mat();
    assert mat != null;
    mat.dims(2);
    int dims = mat.dims();
    assert (dims == 1 || dims == 2); // handle only 1- or 2-D arrays
  }

  public OpenCVMatWritable(Mat mat) {
    setMat(mat);
  }

  public void setMat(Mat mat) throws IllegalArgumentException {
    if (mat == null) {
      throw new IllegalArgumentException("Must provide valid non-null Mat object.");
    }
    int dims = mat.dims();
    if (!(dims == 1 || dims == 2)) {
      throw new IllegalArgumentException("Currently supports only 1D or 2D arrays. Input mat dims: " + dims);
    }
    this.mat = mat;
  }

  public Mat getMat() {
    return mat;
  }

  public void write(DataOutput out) throws IOException {

    assert mat != null;
    int dims = mat.dims();
    assert (dims == 1 || dims == 2); // handle only 1- or 2-D arrays

    int type = mat.type();
    out.writeInt(type);
    out.writeInt(mat.rows());
    out.writeInt(mat.cols());

    int elms = (int)(mat.total() * mat.channels());
    if (elms > 0) {
      int depth =  opencv_core.CV_MAT_DEPTH(type);
      switch (depth) {
        case opencv_core.CV_8U:
        case opencv_core.CV_8S:
          byte [] data = new byte[elms];
          mat.data().get(data);
          out.write(data);
          break;
        case opencv_core.CV_16U:
        case opencv_core.CV_16S:      
          byte [] shortData = new byte[elms * 2];
          mat.data().get(shortData);
          out.write(shortData);
          break;
        case opencv_core.CV_32S:
          byte [] intData = new byte[elms * 4];
          mat.data().get(intData);
          out.write(intData);
          break;
        case opencv_core.CV_32F:
          byte [] floatData = new byte[elms * 4];
          mat.data().get(floatData);
          out.write(floatData);
          break;
        case opencv_core.CV_64F:
          byte [] doubleData = new byte[elms * 8];
          mat.data().get(doubleData);
          out.write(doubleData);
          break;
        default:
          throw new IOException("Unsupported matrix type [" + type + "].");
      }
    }

  }

  public void readFields(DataInput in) throws IOException {
    int type = in.readInt();
    int depth = opencv_core.CV_MAT_DEPTH(type);
    int rows = in.readInt();
    int cols = in.readInt();

    mat = new Mat(rows, cols, type);
    
    int elms = (int)(mat.total() * mat.channels());
    
    
    
    switch (depth) {
      case opencv_core.CV_8U:
      case opencv_core.CV_8S:
        byte[] data = new byte[elms];
        in.readFully(data);
        mat = mat.data(new BytePointer(data));
        break;
      case opencv_core.CV_16U:
      case opencv_core.CV_16S:
        byte[] shortDataAsBytes = new byte[elms * 2]; // 2 bytes per short
        in.readFully(shortDataAsBytes);
        mat = mat.data(new BytePointer(shortDataAsBytes));
        break;
      case opencv_core.CV_32S:
        byte[] intDataAsBytes = new byte[elms * 4]; // 4 bytes per int
        in.readFully(intDataAsBytes);
        mat = mat.data(new BytePointer(intDataAsBytes));
        break;
      case opencv_core.CV_32F:
        byte[] floatDataAsBytes = new byte[elms * 4]; // 4 bytes per float
        in.readFully(floatDataAsBytes);
        mat = mat.data(new BytePointer(floatDataAsBytes));
        break;
      case opencv_core.CV_64F:
        byte[] doubleDataAsBytes = new byte[elms * 8]; // 8 bytes per double
        in.readFully(doubleDataAsBytes);
        mat = mat.data(new BytePointer(doubleDataAsBytes));
        break;
      default:
        throw new IOException("Unsupported matrix type [" + type + "].");
    }
  }


}
