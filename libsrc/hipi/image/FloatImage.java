package hipi.image;

import hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A 2D image represented as an array of floats. A FloatImage consists
 * of a 2D array of pixel values represented as Java floats in
 * addition to the image's spatial resoultion (width and height) and
 * number of bands (color channels). FloatImage supports simple image
 * operations like retrieving and setting individual pixel values,
 * cropping, scaling, and adding two FloatImages together.<br/><br/>
 *
 * The {@link hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) FloatImage objects in various
 * compressed and uncompressed image formats such as JPEG.
 */
public class FloatImage implements Writable, RawComparator<BinaryComparable> {

  private int _w;
  private int _h;
  private int _b;
  private float[] _pels;

  /**
   * Creates an unitialized FloatImage with width = height = bands = 0
   * and does not allocate any memory.
   */
  public FloatImage() {
    _w = _h = _b = 0;
    _pels = null;
  }

  /**
   * Creates a FloatImage of size width x height x bands and performs
   * a shallow copy of the provided float arraay.
   */
  public FloatImage(int width, int height, int bands, float[] pels) {
    _w = width;
    _h = height;
    _b = bands;
    _pels = pels;
  }

  /**
   * Creates a FloatImage of size width x height x bands and allocates
   * a float array of that size.
   */
  public FloatImage(int width, int height, int bands) {
    this(width, height, bands, new float[width * height * bands]);
  }

  /**
   * Compares two FloatImage objects for equality.
   *
   * @return True if the two images are found to deviate by less than
   * 1.0/255.0 at each pixel and across each band, false otherwise.
   */
  @Override
  public boolean equals(Object that) {
    if (this == that)
      return true;
    if (!(that instanceof FloatImage))
      return false;
    FloatImage thatImage = (FloatImage) that;
    if (thatImage.getWidth() == _w && thatImage.getHeight() == _h && thatImage.getBands() == _b) {
      float delta = 1.0f / 255.0f;
      float[] pels = thatImage.getData();
      for (int i = 0; i < _w * _h * _b; i++) {
        if (Math.abs(_pels[i] - pels[i]) > delta) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Crops a float image according the the x,y location and the width, height passed in.
   * 
   * @return a {@link FloatImage} containing the cropped portion of the original image
   */
  public FloatImage crop(int x, int y, int width, int height) {
    float[] pels = new float[width * height * _b];
    for (int i = y; i < y + height; i++)
      for (int j = x * _b; j < (x + width) * _b; j++)
        pels[(i - y) * width * _b + j - x * _b] = _pels[i * _w * _b + j];
    return new FloatImage(width, height, _b, pels);
  }

  /**
   * Constant indicating conversion from RGB to grayscale. Used with {@link #convert}.
   */
  public static final int RGB2GRAY = 0x01;

  /**
   * Performs a color space conversion.
   *
   * @param type Indicates type of color space conversion. Currently
   * must be {@link #RGB2GRAY}.
   * 
   * @return A {@link FloatImage} of the converted image. Returns null
   *         if the image could not be converted.
   */
  public FloatImage convert(int type) {
    switch (type) {
      case RGB2GRAY:
        float[] pels = new float[_w * _h];
        for (int i = 0; i < _w * _h; i++)
          pels[i] = _pels[i * _b] * 0.30f + _pels[i * _b + 1] * 0.59f + _pels[i * _b + 2] * 0.11f;
        return new FloatImage(_w, _h, 1, pels);
    }
    return null;
  }

  /**
   * Performs in-place addition of {@link FloatImage} and the current image.
   * 
   * @param image Target image to add to the current object.
   *
   * @throws IllegalArgumentException If the image dimensions do not match.
   */
  public void add(FloatImage image) throws IllegalArgumentException {
    if (image.getWidth() != _w || image.getHeight() != _h || image.getBands() != _b) {
      throw new IllegalArgumentException("Image dimensions must match.");
    }
    float[] pels = image.getData();
    for (int i = 0; i < _w * _h * _b; i++)
      _pels[i] += pels[i];
  }

  /**
   * Performs in-place addition of a scalar to each band of every
   * pixel.
   * 
   * @param number Scalar to add to each band of each pixel.
   */
  public void add(float number) {
    for (int i = 0; i < _w * _h * _b; i++)
      _pels[i] += number;
  }

  /**
   * Performs in-place pairwise multiplication of {@link FloatImage}
   * and the current image.
   *
   * @param image Target image to use for  multiplication.
   */
  public void scale(FloatImage image) throws IllegalArgumentException {
    if (image.getWidth() != _w || image.getHeight() != _h || image.getBands() != _b) {
      throw new IllegalArgumentException("Image dimensions must match.");
    }
    float[] pels = image.getData();
    for (int i = 0; i < _w * _h * _b; i++)
      _pels[i] *= pels[i];
  }

  /**
   * Performs in-place multiplication with scalar.
   *
   * @param value Scalar to multiply with each band of each pixel.
   */
  public void scale(float value) {
    for (int i = 0; i < _w * _h * _b; i++)
      _pels[i] *= value;
  }

  /**
   * Get floating point value at specific pixel location and channel.
   *
   * @param x Horizintal pixel coordinate (between 0 and width-1, inclusive).
   * @param y Vertical pixel coordinate (between 0 and height-1, inclusive).
   * @param c Color channel (between 0 and numbands-1, inclusive).
   *
   * @throws IndexOutOfBoundsException - If pixel coordinates or
   * channel location is negative or exceeds image bounds.
   */
  public float getPixel(int x, int y, int c) throws IndexOutOfBoundsException {
    if (x < 0 || x >= _w || y < 0 || y >= _h || c < 0 || c >= _b) {
      throw new IndexOutOfBoundsException(String.format("Attempted to get pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,c,_w,_h,_b));
    }
    return _pels[c + (x + y * _w) * _b];
  }

  /**
   * Set floating point value at specific pixel location and channel.
   *
   * @param x Horizintal pixel coordinate (between 0 and width-1, inclusive).
   * @param y Vertical pixel coordinate (between 0 and height-1, inclusive).
   * @param c Color channel (between 0 and numbands-1, inclusive).
   *
   * @throws IndexOutOfBoundsException - If pixel coordinates or
   * channel location is negative or exceeds image bounds.
   */
  public void setPixel(int x, int y, int c, float val) throws IndexOutOfBoundsException {
    if (x < 0 || x >= _w || y < 0 || y >= _h || c < 0 || c >= _b) {
      throw new IndexOutOfBoundsException(String.format("Attempted to set pixel (%d,%d,%d) in image with dimensions (%d,%d,%d)",x,y,c,_w,_h,_b));
    }
    _pels[c + (x + y * _w) * _b] = val;
  }

  /**
   * Get width of image.
   *
   * @return Width of image.
   */
  public int getWidth() {
    return _w;
  }

  /**
   * Get height of image.
   *
   * @return Height of image.
   */
  public int getHeight() {
    return _h;
  }

  /**
   * Get number of bands in image.
   *
   * @return Number of bands in image.
   */
  public int getBands() {
    return _b;
  }

  /**
   * Get float array of image pixel data.
   *
   * @return Pixel float array.
   */
  public float[] getData() {
    return _pels;
  }

  /**
   * Computes hash of float array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  public String hex() {
    return ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(_pels));
  }

  /**
   * Reads an image stored in a simple uncompressed binary format. The
   * first three bytes are the width, height, and number of bands,
   * followed by an array of floating point values (32 bits per
   * channel) of the image pixel data in raster scan order.
   *
   * @param input Interface for reading bytes from a binary stream.
   * @throws IOException
   */
  public void readFields(DataInput input) throws IOException {
    _w = input.readInt();
    _h = input.readInt();
    _b = input.readInt();
    byte[] pixel_buffer = new byte[_w * _h * _b * 4];
    input.readFully(pixel_buffer);
    _pels = ByteUtils.ByteArraytoFloatArray(pixel_buffer);
  }

  /**
   * Writes image in a simple uncompressed binary format.
   *
   * @param output Interface for writing bytes to a binary stream.
   * @throws IOException
   * @see #readFields
   */
  public void write(DataOutput output) throws IOException {
    output.writeInt(_w);
    output.writeInt(_h);
    output.writeInt(_b);
    output.write(ByteUtils.FloatArraytoByteArray(_pels));
  }

  /**
   * Produces a string representation of the image. Concatenates image
   * dimensions with pixel data in lexicographic order.
   *
   * @return String representation of image.
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(_w + " " + _h + " " + _b + "\n");
    for (int i = 0; i < _h; i++) {
      for (int j = 0; j < _w * _b; j++) {
        result.append(_pels[i * _w * _b + j]);
        if (j < _w * _b - 1)
          result.append(" ");
      }
      result.append("\n");
    }
    return result.toString();
  }

  /**
   * Compare method from the {@link RawComparator} interface. This
   * method enables faster sorting than the standard {@link java.util.Comparator}
   * interface. For a discussion, see <a target="_blank" href="http://www.amazon.com/Hadoop-Definitive-Guide-Tom-White/dp/1449311520">Hadoop:
   * The Definitive Guide</a>.  In short, this method avoids
   * deserializing the entire FloatImage object before performing the
   * comparison. Because the first few bytes store the image dimensions, this function only reads
   * this small segment at the beginning of the array and uses these sizes to
   * compare the two images.
   *
   * TODO: Ensure that the second and fifth parameters define the correct starting offset.
   *
   * @return An integer result of the comparison.
   */
  public int compare(byte[] byte_array1, int start1, int length1, byte[] byte_array2, int start2,
      int length2) {
    int w1 = ByteUtils.ByteArrayToInt(byte_array1, start1);
    int w2 = ByteUtils.ByteArrayToInt(byte_array2, start2);

    int h1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 4);
    int h2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 4);

    int b1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 8);
    int b2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 8);

    int size1 = w1 * h1 * b1;
    int size2 = w2 * h2 * b2;

    return (size1 - size2);
  }

  /**
   * Compare method from the {@link java.util.Comparator}
   * interface. This method reads both {@link BinaryComparable}
   * objects into byte arrays and calls {@link #compare}.
   *
   * @return An integer result of the comparison.
   * @see #compare
   */
  public int compare(BinaryComparable o1, BinaryComparable o2) {
    byte[] b1 = o1.getBytes();
    byte[] b2 = o2.getBytes();
    int length1 = o1.getLength();
    int length2 = o2.getLength();

    return compare(b1, 0, length1, b2, 0, length2);
  }

  /**
   * Sets the current object to be equal to another
   * FloatImage. Performs a shallow copy of the image pixel data
   * array.
   *
   * @param image Target image.
   */
  public void set(FloatImage image) {
    this._w = image.getWidth();
    this._h = image.getHeight();
    this._b = image.getBands();
    this._pels = image.getData();
  }
}
