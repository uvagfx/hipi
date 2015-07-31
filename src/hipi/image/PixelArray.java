package hipi.image;

// Derived from http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/6-b27/java/awt/image/DataBuffer.java

public abstract class PixelArray {

  public static final int TYPE_BYTE      = 0;
  public static final int TYPE_USHORT    = 1;
  public static final int TYPE_SHORT     = 2;
  public static final int TYPE_INT       = 3;
  public static final int TYPE_FLOAT     = 4;
  public static final int TYPE_DOUBLE    = 5;
  public static final int TYPE_UNDEFINED = 32;

  private static final int dataTypeSize[] = {1,2,2,4,4,8};

  protected int dataType;
  protected int size;

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
