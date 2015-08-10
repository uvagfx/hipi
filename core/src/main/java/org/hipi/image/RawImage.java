package org.hipi.image;

import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.util.ByteUtils;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.IllegalArgumentException;

/**
 * Concrete class that represents a raw byte representation of an image. These bytes are usually a
 * compressed and encoded representation of the image. The storage format (if known) can be accessed
 * through the object's {@link HipiImageHeader}.
 */
public class RawImage extends HipiImage {

  protected byte[] imageBytes;

  public RawImage() {
    super();
    imageBytes = null;
  }

  public void setRawBytes(byte[] imageBytes) {
    this.imageBytes = imageBytes;
  }

  public byte[] getRawBytes() {
    return imageBytes;
  }

  /**
   * Get image type identifier.
   *
   * @return HipiImageType.RAW
   */
  public HipiImageType getType() {
    return HipiImageType.RAW;
  }

  /**
   * Produces a string representation of the image that concatenates
   * image dimensions with RGB values of up to first 10 pixels in
   * raster-scan order.
   *
   * @see java.lang.Object#toString
   */
  @Override
  public String toString() {
    String typeString = "RawImage";
    int w = getWidth();
    int h = getHeight();
    int b = getNumBands();
    StringBuilder result = new StringBuilder();
    result.append(String.format("%s: %d x %d x %d [", typeString, w, h, b));
    int n = (imageBytes == null ? 0 : Math.min(10,imageBytes.length));
    for (int i=0; i<n; i++) {
      result.append(String.format("%0x",imageBytes[i]));
    }
    result.append("]");
    return result.toString();
  }

  /**
   * Sets the current object to be equal to another RawImage. Performs a shallow copy of the image
   * header and raw image byte array.
   *
   * @param image Target image.
   */
  public void set(RawImage image) {
    this.header = image.header;
    this.imageBytes = image.imageBytes;
  }

  /**
   * Serializes a raw image using a simple uncompressed binary format.
   *
   * @throws IOException if object serialization fails for any reason
   *
   * @see RawImage#readFields
   * @see org.apache.hadoop.io.Writable#write
   */
  @Override
  public void write(DataOutput output) throws IOException {
    if (header == null) {
      throw new IOException("Cannot serialize image object with null image header.");
    }
    header.write(output);
    if (imageBytes == null) {
      output.writeInt(0);
    } else {
      output.writeInt(imageBytes.length);
      output.write(imageBytes,0,imageBytes.length);
    }
  }

  /**
   * Deserializes a raw image using a simple uncompressed binary format.
   *
   * @throws IOException if object deserialization fails for any reason
   *
   * @see org.apache.hadoop.io.Writable#readFields
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    // Create and read header
    header = new HipiImageHeader(input);
    // Read length of raw image byte array
    int n = input.readInt();
    if (n == 0) {
      imageBytes = null;
    } else {
      // Read raw image byte array itself
      imageBytes = new byte[n];
      input.readFully(imageBytes);
    }
  }

  /**
   * Hash of image data.
   *
   * @return hash of raw pixel data as String
   */
  public String hex() {
    return ByteUtils.asHex(imageBytes);
  }

}
