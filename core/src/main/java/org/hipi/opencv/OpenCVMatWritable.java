package org.hipi.opencv;

import org.hipi.util.ByteUtils;

import org.apache.hadoop.io.Writable;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

/**
 * Implementation of Hadoop {@link Writable} interface which encapsulates {@link Mat} objects.
 */
public class OpenCVMatWritable implements Writable {

  private Mat mat = null;

  public OpenCVMatWritable() {
    mat = new Mat();
    assert mat != null;
    mat.dims(2);
    assert (mat.dims() == 2); // handle only 1- or 2-D arrays (sanity check)
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
      throw new IllegalArgumentException("Currently supports only 1D or 2D arrays. "
          + "Input mat dims: " + dims);
    }
    
    Mat matCopy = new Mat(mat.rows(), mat.cols(), mat.type());
    mat.copyTo(matCopy);
    this.mat = matCopy;
  }

  public Mat getMat() {
    Mat matCopy = new Mat(mat.rows(), mat.cols(), mat.type());
    mat.copyTo(matCopy);
    return matCopy;
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
          ((ByteBuffer)mat.createBuffer()).get(data);
          out.write(data);
          break;
        case opencv_core.CV_16U:
        case opencv_core.CV_16S:      
          short [] shortData = new short[elms];
          ((ShortBuffer)mat.createBuffer()).get(shortData);
          out.write(ByteUtils.shortArrayToByteArray(shortData));
          break;
        case opencv_core.CV_32S:
          int [] intData = new int[elms];
          ((IntBuffer)mat.createBuffer()).get(intData);
          out.write(ByteUtils.intArrayToByteArray(intData));
          break;
        case opencv_core.CV_32F:
          float [] floatData = new float[elms];
          ((FloatBuffer)mat.createBuffer()).get(floatData);
          out.write(ByteUtils.floatArrayToByteArray(floatData));
          break;
        case opencv_core.CV_64F:
          double [] doubleData = new double[elms];
          ((DoubleBuffer)mat.createBuffer()).get(doubleData);
          out.write(ByteUtils.doubleArrayToByteArray(doubleData));
          break;
        default:
          throw new IOException("Unsupported matrix depth [" + depth + "].");
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
        ((ByteBuffer)mat.createBuffer()).put(data);
        break;
      case opencv_core.CV_16U:
      case opencv_core.CV_16S:
        byte[] shortDataAsBytes = new byte[elms * 2]; // 2 bytes per short
        in.readFully(shortDataAsBytes);
        ((ShortBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToShortArray(shortDataAsBytes));
        break;
      case opencv_core.CV_32S:
        byte[] intDataAsBytes = new byte[elms * 4]; // 4 bytes per int
        in.readFully(intDataAsBytes);
        ((IntBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToIntArray(intDataAsBytes));
        break;
      case opencv_core.CV_32F:
        byte[] floatDataAsBytes = new byte[elms * 4]; // 4 bytes per float
        in.readFully(floatDataAsBytes);
        ((FloatBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToFloatArray(floatDataAsBytes));
        break;
      case opencv_core.CV_64F:
        byte[] doubleDataAsBytes = new byte[elms * 8]; // 8 bytes per double
        in.readFully(doubleDataAsBytes);
        ((DoubleBuffer)mat.createBuffer()).put(ByteUtils.byteArrayToDoubleArray(doubleDataAsBytes));
        break;
      default:
        throw new IOException("Unsupported matrix depth [" + depth + "].");
    }
  }


}
