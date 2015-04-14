package hipi.image;

import hipi.image.ImageHeader;
import hipi.image.HipiImageException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A raster (uncompressed) 2D image in HIPI. A raster image consists
 * of an ImageHeader and a flat array of the image pixel data stored
 * in interleaved raster-scan order (e.g., RGBRGBRGB...).. This class
 * is templated based on the underlying pixel data type (e.g., byte,
 * float, etc.) and implements the {@link Writable} and {@link
 * RawComparator} interfaces.
 */
public abstract class RasterImage<T> implements Writable, RawComparator<BinaryComparable> {

  protected ImageHeader header;
  protected T[] pels;

  /**
   * Creates a raster image with provided header and pixel data array.
   */
  public RasterImage(ImageHeader header, T[] pels) throws IllegalArgumentException {
    if (pels == null) {
      throw new IllegalArgumentException("Pixel data array is null.");
    }
    if (pels.length != header.getWidth()*header.getHeight()*header.getNumChannels()) {
      throw new IllegalArgumentException("Length of pixel data array does not match image dimensions in header.");
    }
    this.header = header;
    this.pels = pels;
  }

  /**
   * Creates a raster image of specified size and allocates pixel array.
   */
  public RasterImage(int width, int height, int bands) throws IllegalArgumentException {
    this.header = new Header(ImageFormat.UNDEFINED,ColorSpace.getDefault(), width, height, bands, null);
    this.pels = new T[width*height*bands](0);
  }

  /**
   * Helper routine that converts an integer to a value in the pixel
   * data type.
   *
   * @return Converted value in the pixel data type.
   */
  public abstract T convertFromInt(int value);
  
  /**
   * Helper routine that converts a value in the pixel data type to an
   * integer.
   *
   * @return Converted value as a Java int.
   */
  public abstract int convertToInt(T value);

  /**
   * Compares two raster images for equality.
   *
   * @return true if the two images are found to deviate by an amount
   * that is not representable in the underlying pixel type, false
   * otherwise.
   */
  @Override
  public abstract boolean equals(Object that);

  /**
   * Crops a raster image to a (width x height) rectangular region
   * with top-left corner at (x,y) pixel location.
   * 
   * @return a {@link RasterImage} containing the cropped portion of
   * the original image
   */
  public RasterImage<T> crop(int x, int y, int width, int height) throws IllegalArgumentException {
    if (x < 0 || width <= 0 || x+width > this.width ||
	y < 0 || height <= 0 || y+height > this.height) {
      throw new IllegalArgumentException("Invalid crop region.");
    }
    int w = header.getWidth();
    int b = header.getNumBands();
    T[] pels = new T[width*height*b];
    for (int i=y; i<y+height; i++)
      for (int j =x*b; j<(x+width)*b; j++)
        pels[(i-y)*width*b + j-x*b] = pels[i*w*b + j];
    return new RasterImage<T>(width, height, b, pels);
  }

  /**
   * Convert image to another color space.
   *
   * @param colorSpace Indicates target color space.
   * 
   * @return A {@link RasterImage} of the converted image. Returns
   * null if the conversion could not be performed.
   */
  public RasterImage convertToColorSpace(ColorSpace colorSpace) throws IllegalArgumentException {
    if (header.getColorSpace() == colorSpace) {
      throw new IllegalArgumentException("Cannot convert color space to itself.");
    }
    throw new IllegalArgumentException("Not implemented.");
    return null;
    /*
    switch (type) {
      case RGB2GRAY:
        float[] pels = new float[_w * _h];
        for (int i = 0; i < _w * _h; i++)
          pels[i] = _pels[i * _b] * 0.30f + _pels[i * _b + 1] * 0.59f + _pels[i * _b + 2] * 0.11f;
        return new FloatImage(_w, _h, 1, pels);
    }
    return null;
    */
  }

  protected void checkCompatibleInputImage(RasterImage image) throws IllegalArgumentException {
    if (image.getColorSpace() != this.getColorSpace() || image.getWidth() != this.getWidth() || 
	image.getHeight() != this.getHeight() || image.getBands() != this.getNumBands()) {
      throw new IllegalArgumentException("Color space and/or image dimensions do not match.");
    }
  }

  /**
   * Performs in-place addition of a {@link RasterImage} and the
   * current image.
   * 
   * @param image Target image to add to the current object.
   *
   * @throws IllegalArgumentException If the image dimensions do not match.
   */
  public abstract void add(RasterImage image) throws IllegalArgumentException;

  /**
   * Performs in-place addition of a constant to each band of every pixel.
   * 
   * @param number Constant to add to each band of each pixel.
   */
  public abstract void add(T number);

  /**
   * Performs in-place pairwise multiplication of {@link RasterImage}
   * and the current image.
   *
   * @param image Target image to use for  multiplication.
   */
  public abstract void multiply(RasterImage image) throws IllegalArgumentException;

  /**
   * Performs in-place multiplication with scalar.
   *
   * @param value Scalar to multiply with each band of each pixel.
   */
  public abstract void scale(float value);

  /**
   * Get pixel value at specific location and band.
   *
   * @param x Horizintal pixel coordinate (between 0 and width-1, inclusive).
   * @param y Vertical pixel coordinate (between 0 and height-1, inclusive).
   * @param c Color band (between 0 and numBands-1, inclusive).
   *
   * @throws IndexOutOfBoundsException - If pixel coordinates or band
   * is negative or exceeds image dimensions.
   */
  public T getPixel(int x, int y, int b) throws IndexOutOfBoundsException {
    int w = header.getWidth();
    int h = header.getHeight();
    int bands = header.getNumBands();
    if (x < 0 || x >= w || y < 0 || y >= h || b < 0 || b >= bands) {
      throw new IndexOutOfBoundsException(String.format("Attempted to access pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,b,w,h,bands));
    }
    return pels[b + (x + y * w) * bands]
  }

  /**
   * Set pixel value at specific location and band.
   *
   * @param x Horizintal pixel coordinate (between 0 and width-1, inclusive).
   * @param y Vertical pixel coordinate (between 0 and height-1, inclusive).
   * @param b Color band (between 0 and numBands-1, inclusive).
   *
   * @throws IndexOutOfBoundsException - If pixel coordinates or band
   * is negative or exceeds image dimensions.
   */
  public void setPixel(int x, int y, int b, T val) throws IndexOutOfBoundsException {
    int w = header.getWidth();
    int h = header.getHeight();
    int bands = header.getNumBands();
    if (x < 0 || x >= w || y < 0 || y >= h || b < 0 || b >= bands) {
      throw new IndexOutOfBoundsException(String.format("Attempted to set pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,b,w,h,bands));
    }
    pels[b + (x + y * w) * bands] = val;
  }

  /**
   * Get storage type of image.
   *
   * @return Storage type of image.
   */
  public ImageType getStorageType() {
    return header.getStorageType();
  }

  /**
   * Get color space of image.
   *
   * @return Color space of image.
   */
  public int getColorSpace() {
    return header.getColorSpace();
  }

  /**
   * Get width of image.
   *
   * @return Width of image.
   */
  public int getWidth() {
    return header.getWidth();
  }

  /**
   * Get height of image.
   *
   * @return Height of image.
   */
  public int getHeight() {
    return header.getHeight();
  }

  /**
   * Get number of bands in image.
   *
   * @return Number of bands in image.
   */
  public int getNumBands() {
    return header.getNumBands();
  }

  /**
   * Get array of image pixel data.
   *
   * @return Pixel data array.
   */
  public T[] getData() {
    return pels;
  }

  /**
   * Computes hash of array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  public abstract String hex();

  /**
   * Produces a string representation of the image. Concatenates image
   * header with pixel data in interleaved lexicographic order.
   *
   * @return String representation of image.
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.appen(header.toString());
    result.append("\n");
    int w = header.getWidth();
    int h = header.getHeight();
    int b = header.getNumBands();
    for (int i = 0; i < h; i++) {
      for (int j = 0; j < w * b; j++) {
        result.append(pels[i * w * b + j]);
        if (j < w * b - 1)
          result.append(" ");
      }
      result.append("\n");
    }
    return result.toString();
  }

  /**
   * Compare method from the {@link java.util.Comparator}
   * interface. Currently calls the corresponding compare method in
   * the {@link ImageHeader} class.
   *
   * @return An integer result of the comparison.
   */
  public int compare(BinaryComparable o1, BinaryComparable o2) {
    return ImageHeader.compare(o1,o2);
  }

  /**
   * Compare method from the {@link RawComparator}
   * interface. Currently calls the corresponding compare method in
   * the {@link ImageHeader} class.
   *
   * @return An integer result of the comparison.
   */
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
    return ImageHeader.compare(arg0, arg1, arg2, arg3, arg4, arg5);
  }

  /**
   * Sets the current object to be equal to another
   * RasterImage. Performs a shallow copy of the image header and
   * pixel data array.
   *
   * @param image Target image.
   */
  public void set(FloatImage image) {
    this.header = image.header;
    this.pels = image.pels;
  }

} // public abstract class RasterImage<T>...
