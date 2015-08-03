package org.hipi.image;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.util.ByteUtils;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import javax.imageio.metadata.IIOMetadata;

/**
 * A 2D image object in HIPI. This class implements the Writable
 * interface so that it can be used as a value object in MapReduce.
 * @see org.apache.hadoop.io.Writable
 */

public abstract class HipiImage implements Writable {

  public enum HipiImageType {
    UNDEFINED(0x0), FLOAT(0x1), BYTE(0x2), RAW(0x3), OPENCV(0x4);

    private int type;

    /**
     * Creates a HipiImageType from an int.
     *
     * @param format Integer representation of HipiImageType.
     */
    HipiImageType(int type) {
      this.type = type;
    }

    /**
     * Creates a HipiImageType from an int.
     *
     * @param val Integer representation of HipiImageType.
     *
     * @return Associated HipiImageType.
     */
    public static HipiImageType fromInteger(int type) throws IllegalArgumentException {
      for (HipiImageType typ : values()) {
	if (typ.type == type) {
	  return typ;
	}
      }
      throw new IllegalArgumentException(String.format("There is no HipiImageType enum value associated with integer [%d]", type));
    }

    /** 
     * @return Integer representation of HipiImageType
     */
    public int toInteger() {
      return type;
    }

    /**
     * Default HipiImageType.
     *
     * @return HipiImageType.UNDEFINED
     */
    public static HipiImageType getDefault() {
      return UNDEFINED;
    }

  } // public enum HipiImageType

  protected HipiImageHeader header;

  protected HipiImage() {
    this.header = null;
  }

  public void setHeader(HipiImageHeader header) throws IllegalArgumentException {
    if (header == null) {
      throw new IllegalArgumentException("Image header must not be null.");
    }
    if (header.getWidth() <= 0 || header.getHeight() <= 0 || header.getNumBands() <= 0) {
      throw new IllegalArgumentException(String.format("Invalid dimensions in image header: [w:%d x h:%d x b:%d]", 
						       header.getWidth(), header.getHeight(), header.getNumBands()));
    }
    this.header = header;
  }

  /**
   * Get object type identifier.
   *
   * @return Type of object.
   */
  public HipiImageType getType() {
    return HipiImageType.UNDEFINED;
  }

  /**
   * Get storage format of image.
   *
   * @return Storage format of image.
   */
  public HipiImageFormat getStorageFormat() {
    return header.getStorageFormat();
  }

  /**
   * Get color space of image.
   *
   * @return Color space of image.
   */
  public HipiColorSpace getColorSpace() {
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
   * Get meta data for particular key.
   *
   * @return meta data value as String
   */
  public String getMetaData(String key) {
    return header.getMetaData(key);
  }

  /**
   * Get all of the metadata associated with this image as {@link
   * HashMap}.
   *
   * @return a hash map containing the keys and values of the metadata
   */
  public HashMap<String, String> getAllMetaData() {
    return header.getAllMetaData();
  }

  /**
   * Get EXIF data value for particular key.
   *
   * @return EXIF data object as String
   */
  public String getExifData(String key) {
    return header.getExifData(key);
  }

  /**
   * Get the entire map of EXIF data.
   *
   * @return a hash map containing the keys and values of the metadata
   */
  public HashMap<String, String> getAllExifData() {
    return header.getAllExifData();
  }

  /**
   * Computes hash of array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  public abstract String hex();

}
