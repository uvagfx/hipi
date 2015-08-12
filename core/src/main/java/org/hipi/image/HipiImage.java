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
 * An abstract base class from which all concrete image classes in HIPI must be derived. This class
 * implements the {@link org.apache.hadoop.io.Writable} interface so that it can be used as a value
 * object in a MapReduce program.
 */
public abstract class HipiImage implements Writable {

  /**
   * Enumeration of the supported image object types in HIPI (e.g., FloatImage, ByteImage, etc.).
   */
  public enum HipiImageType {
    UNDEFINED(0x0), FLOAT(0x1), BYTE(0x2), RAW(0x3);

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
     * @param type integer representation of HipiImageType
     *
     * @return Associated HipiImageType.
     *
     * @throws IllegalArgumentException if the parameter does not correspond to a valid
     * HipiImageType.
     */
    public static HipiImageType fromInteger(int type) throws IllegalArgumentException {
      for (HipiImageType typ : values()) {
        if (typ.type == type) {
         return typ;
        }
      }
      throw new IllegalArgumentException(String.format("There is no HipiImageType enum value " +
        "associated with integer [%d]", type));
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

  /**
   * Every HipiImage contains a HipiImageHeader that stores universally available information about
   * the image such as its spatial dimensions and color space.
   */
  protected HipiImageHeader header;

  /**
   * Default constructor. Sets header field to null.
   */
  protected HipiImage() {
    this.header = null;
  }

  /**
   * Set value of header field.
   *
   * @param header header object to use as source of assignment
   *
   * @throws IllegalArgumentException of the provided header is null or contains invalid values
   */
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
   * Get image type identifier.
   *
   * @return HipiImageType.UNDEFINED
   */
  public HipiImageType getType() {
    return HipiImageType.UNDEFINED;
  }

  /**
   * Get storage format of image.
   *
   * @return storage format of image
   */
  public HipiImageFormat getStorageFormat() {
    return header.getStorageFormat();
  }

  /**
   * Get color space of image.
   *
   * @return color space of image
   */
  public HipiColorSpace getColorSpace() {
    return header.getColorSpace();
  }

  /**
   * Get width of image.
   *
   * @return width of image
   */
  public int getWidth() {
    return header.getWidth();
  }

  /**
   * Get height of image.
   *
   * @return height of image
   */
  public int getHeight() {
    return header.getHeight();
  }

  /**
   * Get number of bands (also called "channels") in image.
   *
   * @return number of color bands in image
   */
  public int getNumBands() {
    return header.getNumBands();
  }

  /**
   * Get meta data value for particular key.
   *
   * @return meta data value as String (null if key does not exist in meta data dictionary)
   */
  public String getMetaData(String key) {
    return header.getMetaData(key);
  }

  /**
   * Get the entire image meta data dictionary as a {@link HashMap}.
   *
   * @return a hash map containing the image meta data key/value pairs
   */
  public HashMap<String, String> getAllMetaData() {
    return header.getAllMetaData();
  }

  /**
   * Get EXIF data value for particular key.
   *
   * @return EXIF data object as String (null if key does not exist in EXIF data dictionary)
   */
  public String getExifData(String key) {
    return header.getExifData(key);
  }

  /**
   * Get the entire EXIF dictionary as a {@link HashMap}.
   *
   * @return a hash map containing the EXIF image data key/value pairs
   */
  public HashMap<String, String> getAllExifData() {
    return header.getAllExifData();
  }

  /**
   * Hash of image data.
   *
   * @return hash of pixel data as String
   */
  public abstract String hex();

}
