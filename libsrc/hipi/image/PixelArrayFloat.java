package hipi.image;

import hipi.image.PixelArray;

public class PixelArrayFloat extends PixelArray {

  float data[];

  public PixelArrayFloat(int size) {
    super(TYPE_FLOAT, size);
    data = new float[size];
  }

  public float[] getData() {
    return data;
  }

  public int getElem(int i) {
    return (int)(data[i+offset]);
  }

  public void setElem(int i, int val) {
    data[i] = (float)val;
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

  public void setElemDouble(int bank, int i, double val) {
    data[i] = (float)val;
  }

}
