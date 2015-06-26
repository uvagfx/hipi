package hipi.image;

import hipi.image.ImageHeader;
import hipi.image.HipiImageException;
import hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A 2D image object in HIPI.
 */
public abstract class HipiImage implements Writable, RawComparator<BinaryComparable> {

  protected ImageHeader header;

  protected HipiImage(ImageHeader header) throws IllegalArgumentException, RuntimeException {
    this.header = header;
  }

  /**
   * Get storage format of image.
   *
   * @return Storage format of image.
   */
  public ImageFormat getStorageFormat() {
    return header.getStorageFormat();
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
   * Computes hash of array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  public abstract String hex();

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

}
