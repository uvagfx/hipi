package hipi.image;

import hipi.image.PixelArray;

public class PixelArrayByte extends PixelArray {

  byte data[];

  public PixelArrayByte(int size) {
    super(TYPE_BYTE, size);
    data = new byte[size];
  }

  public byte[] getData() {
    return data;
  }

  public int getElem(int i) {
    return (int)(data[i]) & 0xff;
  }

  public void setElem(int i, int val) {
    data[i] = (byte)val;
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
