package hipi.image;

import hipi.image.PixelArray;
import hipi.util.ByteUtils;

public class PixelArrayFloat extends PixelArray {

  float data[];

  public PixelArrayFloat() {
    super(TYPE_FLOAT, 0);
    data = null;
  }

  public PixelArrayFloat(int size) {
    super(TYPE_FLOAT, size);
    data = new float[size];
  }

  public float[] getData() {
    return data;
  }

  public void setSize(int size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException("Invalid size of pixel array.");
    }
    this.size = size;
    if (size == 0) {
      this.data = null;
    } else {
      this.data = new float[size];
    }
  }

  public byte[] getByteArray() {
    return ByteUtils.floatArrayToByteArray(data);
  }

  public void setFromByteArray(byte[] bytes) throws IllegalArgumentException {
    if (bytes == null || bytes.length == 0) {
      data = null;
      this.size = 0;
    } else {
      data = ByteUtils.byteArrayToFloatArray(bytes);
      this.size = data.length;
    }
  }

  public int getElem(int i) {
    return (int)(Math.max(0,Math.min(255,(int)(data[i]*255.0f))));
  }

  public void setElem(int i, int val) {
    data[i] = ((float)val)/255.0f;
  }    

  public float getElemFloat(int i) {
    return data[i];
  }

  public void setElemFloat(int i, float val) {
    data[i] = val;
  }

  public double getElemDouble(int i) {
    return (double)data[i];
  }

  public void setElemDouble(int i, double val) {
    data[i] = (float)val;
  }

}
