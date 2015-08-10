package org.hipi.image;

import org.hipi.image.PixelArray;

/**
 * A flat array of image pixel values represented as Java bytes.
 */
public class PixelArrayByte extends PixelArray {

  byte data[];

  public PixelArrayByte(int size) {
    super(TYPE_BYTE, size);
    data = new byte[size];
  }

  public PixelArrayByte() {
    super(TYPE_BYTE, 0);
    data = null;
  }

  public byte[] getData() {
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
      this.data = new byte[size];
    }
  }

  public byte[] getByteArray() {
    return data;
  }

  public void setFromByteArray(byte[] bytes) throws IllegalArgumentException {
    if (bytes == null || bytes.length == 0) {
      data = null;
      this.size = 0;
    } else {
      data = bytes;
      this.size = data.length;
    }
  }

  public int getElem(int i) {
    return data[i] & 0xff;
  }

  public int getElemNonLinSRGB(int i) {
    // Assumes values are stored in gamma compressed non-linear sRGB
    // space
    return getElem(i);
  }

  public void setElem(int i, int val) {
    data[i] = (byte)(Math.max(0,Math.min(255,val)));
  }    

  public void setElemNonLinSRGB(int i, int val) {
    // Assumes values are stored in gamma compressed non-linear sRGB
    // space
    setElem(i,val);
  }

  public float getElemFloat(int i) {
    return (float)(data[i] & 0xff)/255.0f;
  }

  public void setElemFloat(int i, float val) {
    data[i] = (byte)(((int)Math.max(0.0,Math.min(255.0,val*255.0))) & 0xff);
  }

  public double getElemDouble(int i) {
    return (double)(data[i] & 0xff)/255.0;
  }

  public void setElemDouble(int i, double val) {
    data[i] = (byte)(((int)Math.max(0.0,Math.min(255.0,val*255.0))) & 0xff);
  }

}
