package hipi.image;

import hipi.image.PixelArray;
import hipi.image.ImageHeader;
import hipi.image.HipiImage;
import hipi.image.PixelArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A raster (uncompressed) 2D image in HIPI. A raster image consists
 * of an ImageHeader (inherited from the HipiImage abstract base
 * class) and a flat array of uncompressed image pixel data stored in
 * interleaved raster-scan order (e.g., RGBRGBRGB...).
 */
public abstract class RasterImage<T extends PixelArray> extends HipiImage {

  protected T pixelArray;

  /**
   * Creates a raster image with provided header and pixel data array.
   */
  public RasterImage(ImageHeader header) throws IllegalArgumentException {
    if (header.getWidth() <= 0 || header.getHeight() <= 0 || header.getNumBands() <= 0) {
      throw new IllegalArgumentException("Invalid dimensions in image header.");
    }      
    super(header);
    int size = header.getWidth()*header.getHeight()*header.getNumBands();
    this.pixelArray = new T(size);
  }

  /**
   * Creates a raster image of specified size and allocates pixel array.
   */
  public RasterImage(int width, int height, int bands) throws IllegalArgumentException {
    this.header = new Header(ImageFormat.UNDEFINED, ColorSpace.getDefault(), width, height, bands, null);
    int size = width*height*bands;
    this.pixelArray = new T(size);
  }

  /**
   * Creates a raster image of specified size and allocates pixel array.
   */
  public RasterImage(int width, int height, int bands, T pixelArray) throws IllegalArgumentException {
    int size = width*height*bands;
    if (size != pixelArray.size) {
      throw new IllegalArgumentException("Mismatch between pixelArray size and specified image dimensions.");
    }
    this.header = new Header(ImageFormat.UNDEFINED, ColorSpace.getDefault(), width, height, bands, null);
    this.pixelArray = pixelArray;
  }

  public T getPixelArray() {
    return pixelArray;
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
    int w = this.getWidth();
    int b = this.getNumBands();
    T pixelArrayCrop = new T(width*height*b);
    //    T[] pels = new T[width*height*b];
    for (int j=y; j<y+height; j++) {
      for (int i=x; i<x+width; i++) {
	for (int c=0; c<b; c++) {
	  pixelArrayCrop.setElem(((j-y)*width+(i-x))*b+c) = pixelArray.getElem((j*w+i)*b+c);
	}
      }
    }
    
    return new RasterImage<T>(width, height, b, pixelArrayCrop);
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

  /**
   * Helper routine that verifies two images have compatible
   * dimensions for common operations (addition, elementwise
   * multiplication, etc.)
   *
   * @param image RasterImage to check
   * 
   * @throws IllegalArgumentException if the image do not have
   * compatible dimensions. Otherwise has no effect.
   */
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
  /*
  public T getPixel(int x, int y, int b) throws IndexOutOfBoundsException {
    int w = this.getWidth();
    int h = this.getHeight();
    int bands = this.getNumBands();
    if (x < 0 || x >= w || y < 0 || y >= h || b < 0 || b >= bands) {
      throw new IndexOutOfBoundsException(String.format("Attempted to access pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,b,w,h,bands));
    }
    return pels[b + (x + y * w) * bands];
  }
  */

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
  /*
  public void setPixel(int x, int y, int b, T val) throws IndexOutOfBoundsException {
    int w = header.getWidth();
    int h = header.getHeight();
    int bands = header.getNumBands();
    if (x < 0 || x >= w || y < 0 || y >= h || b < 0 || b >= bands) {
      throw new IndexOutOfBoundsException(String.format("Attempted to set pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,b,w,h,bands));
    }
    pels[b + (x + y * w) * bands] = val;
  }
  */

  /**
   * Get array of image pixel data.
   *
   * @return Pixel data array.
   */
  /*
  public T[] getData() {
    return pels;
  }
  */

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
    for (int j=0; j<h; j++) {
      for (int i=0; i<w; i++) {
	for (int c=0; c<b; c++) {
        result.append(T.getElem[(j*w+i)*b+c]);
	result.append(" ");
	}
      }
      result.append("\n");
    }
    return result.toString();
  }

  /**
   * Sets the current object to be equal to another
   * RasterImage. Performs a shallow copy of the image header and
   * pixel data array.
   *
   * @param image Target image.
   */
  public void set(RasterImage<T> image) {
    this.header = image.header;
    this.pixelArray = image.pixelArray;
  }

} // public abstract class RasterImage<T>...
