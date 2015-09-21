package org.hipi.image;

/**
 * An abstract base class representing a flat array of image pixel values. Concrete base classes
 * enforce a particular scalar value data type (e.g., byte, float, double, int, etc.).
 *
 * Adapted from {@link java.awt.image.DataBuffer}.
 */
public abstract class PixelArray {

  public static final int TYPE_BYTE  = 0;
  public static final int TYPE_USHORT = 1;
  public static final int TYPE_SHORT = 2;
  public static final int TYPE_INT   = 3;
  public static final int TYPE_FLOAT  = 4;
  public static final int TYPE_DOUBLE  = 5;
  public static final int TYPE_UNDEFINED = 32;

  private static final int dataTypeSize[] = {1,2,2,4,4,8};

  /**
   * Integer value indicating underlying scalar value data type.
   */
  protected int dataType;

  /**
   * Size, in bytes, of a single scalar value in pixel array.
   */
  protected int size;

  /**
   * Static function that reports size, in bytes, of a single scalar value for different types
   * of pixel arrays.
   *
   * @param type scalar value type
   *
   * @return size, in bytes, of single scalar value for specified type
   */
  public static int getDataTypeSize(int type) {
    if (type < TYPE_BYTE || type > TYPE_DOUBLE) {
      throw new IllegalArgumentException("Unknown data type "+type);
    }
    return dataTypeSize[type];
  }

  public PixelArray() {
    this.dataType = TYPE_UNDEFINED;
    this.size = 0;
  }

  protected PixelArray(int dataType, int size) {
    this.dataType = dataType;
    this.size = size;
  }

  public int getDataType() {
    return dataType;
  }

  public int getSize() {
    return size;
  }

  public abstract void setSize(int size) throws IllegalArgumentException;

  public abstract byte[] getByteArray();

  public abstract void setFromByteArray(byte[] bytes) throws IllegalArgumentException;

  public abstract int getElem(int i);

  public abstract int getElemNonLinSRGB(int i);

  public abstract void setElem(int i, int val);

  public abstract void setElemNonLinSRGB(int i, int val);

  public float getElemFloat(int i) {
    return (float)getElem(i);
  }

  public void setElemFloat(int i, float val) {
    setElem(i,(int)val);
  }

  public double getElemDouble(int i) {
    return (double)getElem(i);
  }

  public void setElemDouble(int i, double val) {
    setElem(i,(int)val);
  }

}
