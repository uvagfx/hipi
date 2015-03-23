package hipi.image;

import hipi.image.ImageHeader;
import hipi.image.AbstractImage;
import hipi.image.HipiImageException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * Abstract base class representing a 2D image in HIPI. An image
 * consists of an ImageHeader and a representation of the pixel data
 * which may be compressed, encoded, both, or neither.
 */
public class FlatImage<T> extends AbstractImage {

  private T[] pels;

  /**
   * Creates an unitialized FloatImage with width = height = bands = 0
   * and does not allocate any memory.
   */
  public FlatImage() {
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


}
