package org.hipi.image;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.RasterImage;
import org.hipi.image.PixelArrayByte;
import org.hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * A raster image represented as an array of Java bytes. A ByteImage consists
 * of a flat array of pixel values represented as a {@link PixelArrayByte} 
 * object along with a {@link HipiImageHeader} object.
 *
 * The {@link org.hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) ByteImage objects in various
 * compressed and uncompressed image formats such as JPEG and PNG.
 */
public class ByteImage extends RasterImage {

  public ByteImage() {
    super((PixelArray)(new PixelArrayByte()));
  }

  /**
   * Get object type identifier.
   *
   * @return Type of object.
   */
  public HipiImageType getType() {
    return HipiImageType.BYTE;
  }

  /**
   * Provides direct access to underlying byte array of pixel data.
   */
  public byte[] getData() {
    return ((PixelArrayByte)this.pixelArray).getData();
  }

  /**
   * Compares two ByteImage objects for equality allowing for some
   * amount of differences in pixel values.
   *
   * @return True if the two images have equal dimensions, color
   * spaces, and are found to deviate by less than a maximum
   * difference, false otherwise.
   */
  public boolean equalsWithTolerance(RasterImage thatImage, 
    float maxDifference) {
    
    if (thatImage == null) {
      return false;
    }

    // Verify dimensions in headers are equal
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    if (this.getColorSpace() != thatImage.getColorSpace() ||
     thatImage.getWidth() != w || thatImage.getHeight() != h || 
     thatImage.getNumBands() != b) {
      return false;
  }

    // Get pointers to pixel arrays
    PixelArray thisPA = this.getPixelArray();
    PixelArray thatPA = thatImage.getPixelArray();

    // Check that pixel data is equal.
    for (int i = 0; i < w*h*b; i++) {
      if ((float)Math.abs(thisPA.getElem(i) - thatPA.getElem(i)) > maxDifference) {
       return false;
      }
    }

    // Passed, declare equality
    return true;
  }

  /**
   * Compares two ByteImage objects for equality.
   *
   * @return True if the two images have equal dimensions, color
   * spaces, and are found to deviate by less than a single intensity
   * value at each pixel and across each band, false otherwise.
   */
  @Override
  public boolean equals(Object that) {
    // Check for pointer equivalence
    if (this == that)
      return true;

    // Verify object types are equal
    if (!(that instanceof ByteImage))
      return false;

    return equalsWithTolerance((ByteImage)that, 0.0f);
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
    return ByteUtils.asHex(getData());
  }

} // public class ByteImage
