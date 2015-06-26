package hipi.image;

import hipi.image.ImageHeader;
import hipi.image.RasterImage;

import hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A 2D image represented as an array of floats. A FloatImage consists
 * of a flat array of pixel values represented as Java floats in
 * addition to an {@link ImageHeader} object.<br/><br/>
 *
 * The {@link hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) FloatImage objects in various
 * compressed and uncompressed image formats such as JPEG.
 */
public class FloatImage extends RasterImage<Float> {

  public FloatImage(ImageHeader header) throws IllegalArgumentException {
    if (header.getWidth() <= 0 || header.getHeight() <= 0 || header.getNumBands() <= 0) {
      throw new IllegalArgumentException("Image dimensions and number of color bands specified in header must be positive.");
    }
    this.header = header;
    //    this.pels = new float[header.getWidth()*header.getHeight()*header.getNumBands()];
    this.pels = new Float[header.getWidth()*header.getHeight()*header.getNumBands()];
  }

  /**
   * Helper routine that converts an integer in the range [0,255] to a
   * float in the range [0,1.0].
   *
   * @return Converted value.
   */
  public float convertFromInt(int value) {
    return ((float)value/255.0);
  }
  
  /**
   * Helper routine that converts a value in the pixel data type to an
   * integer in the range [0,255].
   *
   * @return Converted value.
   */
  public abstract int convertToInt(float value) {
    return Math.min(Math.max((int)(value*255.0),0),255);
  }

  /**
   * Compares two FloatImage objects for equality.
   *
   * @return True if the two images are found to deviate by less than
   * 1.0/255.0 at each pixel and across each band, false otherwise.
   */
  @Override
  public boolean equals(Object that) {
    // Check for pointer equivalence.
    if (this == that)
      return true;

    // Check that object types are equal.
    if (!(that instanceof FloatImage))
      return false;
    FloatImage thatImage = (FloatImage)that;

    // Check that headers are equal.
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    if (this.getColorSpace() != thatImage.getColorSpace() ||
	thatImage.getWidth() != w || thatImage.getHeight() != h || thatImage.getNumBands() != b) {
      return false;
    }

    // Check that pixel data is equal.
    float delta = 1.0f / 255.0f;
    float[] pels = (float[])thatImage.getData();
    for (int i = 0; i < w*h*b; i++) {
      if (Math.abs(pels[i] - pels[i]) > delta) {
	return false;
      }
    }

    // Passed all of our tests.
    return true;
  }

  /**
   * Performs in-place addition of {@link FloatImage} and the current image.
   * 
   * @param image Target image to add to the current object.
   *
   * @throws IllegalArgumentException If the image dimensions do not match.
   */
  @Override
  public void add(FloatImage image) throws IllegalArgumentException {
    // Verify input
    checkCompatibleInputImage(image);

    // Perform in-place addition
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getBands();
    float[] otherPels = image.getData();
    for (int i=0; i<w*h*b; i++) {
      pels[i] += otherPels[i];
    }
  }

  /**
   * Performs in-place addition of a scalar to each band of every
   * pixel.
   * 
   * @param number Scalar to add to each band of each pixel.
   */
  @Override
  public void add(float number) {
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getBands();
    for (int i=0; i<w*h*b; i++) {
      pels[i] += number;
    }
  }

  /**
   * Performs in-place elementwise multiplication of {@link
   * FloatImage} and the current image.
   *
   * @param image Target image to use for  multiplication.
   */
  @Override
  public void multiply(FloatImage image) throws IllegalArgumentException {

    // Verify input
    checkCompatibleInputImage(image);

    // Perform in-place elementwise multiply
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getBands();
    float[] otherPels = image.getData();
    for (int i=0; i<w*h*b; i++) {
      pels[i] *= otherPels[i];
    }
  }

  /**
   * Performs in-place multiplication with scalar.
   *
   * @param value Scalar to multiply with each band of each pixel.
   */
  @Override
  public void scale(float value) {
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getBands();
    for (int i=0; i<w*h*b; i++) {
      pels[i] *= value;
    }
  }

  /**
   * Computes hash of float array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  @Override
  public String hex() {
    return ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(pels));
  }

  /**
   * Reads a float image stored in a simple uncompressed binary
   * format.
   *
   * @param input Interface for reading bytes from a binary stream.
   * @throws IOException
   * @see #write
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    // Read in header
    header.readFields(input);
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getBands();

    // Read in pixel data
    byte[] pixelBuffer = new byte[w*h*b*4]; // 4 bytes per float
    input.readFully(pixelBuffer);
    pels = ByteUtils.ByteArraytoFloatArray(pixelBuffer);
  }

  /**
   * Writes float image in a simple uncompressed binary format.
   *
   * @param output Interface for writing bytes to a binary stream.
   * @throws IOException
   * @see #readFields
   */
  @Override
  public void write(DataOutput output) throws IOException {
    header.write(output);
    output.write(ByteUtils.FloatArraytoByteArray(pels));
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

} // public class FloatImage...
