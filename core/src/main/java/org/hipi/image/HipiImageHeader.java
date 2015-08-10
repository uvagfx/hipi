package org.hipi.image;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The header information for a HipiImage. HipiImageHeader encapsulates universally available
 * information about a 2D image: width, height, storage format, color space, number of color bands 
 * (also called "channels") along with optional image meta data and EXIF data represented
 * as key/value String dictionaries.
 *
 * The {@link org.hipi.image.io} package provides classes for reading (decoding) HipiImageHeader
 * from both {@link org.hipi.imagebundle.HipiImageBundle} files and various standard image storage
 * foramts such as JPEG and PNG.
 *
 * Note that this class implements the {@link org.apache.hadoop.io.WritableComparable} interface,
 * allowing it to be used as a key/value object in MapReduce programs.
 */
public class HipiImageHeader implements WritableComparable<HipiImageHeader> {

  /**
   * Enumeration of the image storage formats supported in HIPI (e.g, JPEG, PNG, etc.).
   */
  public enum HipiImageFormat {
    UNDEFINED(0x0), JPEG(0x1), PNG(0x2), PPM(0x3);

    private int format;

    /**
     * Creates an ImageFormat from an int.
     *
     * @param format Integer representation of ImageFormat.
     */
    HipiImageFormat(int format) {
      this.format = format;
    }

    /**
     * Creates an ImageFormat from an int.
     *
     * @param format Integer representation of ImageFormat.
     *
     * @return Associated ImageFormat.
     *
     * @throws IllegalArgumentException if the parameter value does not correspond to a valid
     * HipiImageFormat.
     */
    public static HipiImageFormat fromInteger(int format) throws IllegalArgumentException {
      for (HipiImageFormat fmt : values()) {
        if (fmt.format == format) {
          return fmt;
        }
      }
      throw new IllegalArgumentException(String.format("There is no HipiImageFormat enum value " +
        "associated with integer [%d]", format));
    }

    /** 
     * @return Integer representation of ImageFormat.
     */
    public int toInteger() {
      return format;
    }

    /**
     * Default HipiImageFormat.
     *
     * @return HipiImageFormat.UNDEFINED
     */
    public static HipiImageFormat getDefault() {
      return UNDEFINED;
    }

  } // public enum ImageFormat

  /**
   * Enumeration of the color spaces supported in HIPI.
   */
  public enum HipiColorSpace {
    UNDEFINED(0x0), RGB(0x1), LUM(0x2);

    private int cspace;

    /**
     * Creates a HipiColorSpace from an int
     *
     * @param format Integer representation of ColorSpace.
     */
    HipiColorSpace(int cspace) {
      this.cspace = cspace;
    }

    /**
     * Creates a HipiColorSpace from an int.
     *
     * @param cspace Integer representation of ColorSpace.
     *
     * @return Associated HipiColorSpace value.
     *
     * @throws IllegalArgumentException if parameter does not correspond to a valid HipiColorSpace.
     */
    public static HipiColorSpace fromInteger(int cspace) throws IllegalArgumentException {
      for (HipiColorSpace cs : values()) {
        if (cs.cspace == cspace) {
	  return cs;
        }
      }
      throw new IllegalArgumentException(String.format("There is no HipiColorSpace enum value " +
        "with an associated integer value of %d", cspace));
    }

    /** 
     * Integer representation of ColorSpace.
     *
     * @return Integer representation of ColorSpace.
     */
    public int toInteger() {
      return cspace;
    }

    /**
     * Default HipiColorSpace. Currently (linear) RGB.
     *
     * @return Default ColorSpace enum value.
     */
    public static HipiColorSpace getDefault() {
      return RGB;
    }

  } // public enum ColorSpace

  private HipiImageFormat storageFormat; // format used to store image on HDFS
  private HipiColorSpace colorSpace;     // color space of pixel data
  private int width;                     // width of image
  private int height;                    // height of image
  private int bands;                     // number of color bands (aka channels)

  /**
   * A map containing key/value pairs of meta data associated with the
   * image. These are (optionally) added during HIB construction and
   * are distinct from the exif data that may be stored within the
   * image file, which is accessed through the IIOMetadata object. For
   * example, this would be the correct place to store the image tile
   * offset and size if you were using a HIB to store a very large
   * image as a collection of smaller image tiles. Another example
   * would be using this dictionary to store the source url for an
   * image downloaded from the Internet.
   */
  private Map<String, String> metaData = new HashMap<String,String>();

  /**
   * EXIF data associated with the image represented as a
   * HashMap. {@see hipi.image.io.ExifDataUtils}
   */
  private Map<String, String> exifData = new HashMap<String,String>();

  /**
   * Creates an ImageHeader.
   */
  public HipiImageHeader(HipiImageFormat storageFormat, HipiColorSpace colorSpace, 
			 int width, int height,
			 int bands, byte[] metaDataBytes, Map<String,String> exifData)
    throws IllegalArgumentException {
    if (width < 1 || height < 1 || bands < 1) {
      throw new IllegalArgumentException(String.format("Invalid spatial dimensions or number " + 
        "of bands: (%d,%d,%d)", width, height, bands));
    }
    this.storageFormat = storageFormat;
    this.colorSpace = colorSpace;
    this.width = width;
    this.height = height;
    this.bands = bands;
    if (metaDataBytes != null) {
      setMetaDataFromBytes(metaDataBytes);
    }
    this.exifData = exifData;
  }

  /**
   * Creates an ImageHeader by calling #readFields on the data input
   * object. Note that this function does not populate the exifData
   * field. That must be done with a separate method call.
   */
  public HipiImageHeader(DataInput input) throws IOException {
    readFields(input);
  }

  /**
   * Get the image storage type.
   *
   * @return Current image storage type.
   */
  public HipiImageFormat getStorageFormat() {
    return storageFormat;
  }

  /**
   * Get the image color space.
   *
   * @return Image color space.
   */
  public HipiColorSpace getColorSpace() {
    return colorSpace;
  }

  /**
   * Get width of image.
   *
   * @return Width of image.
   */
  public int getWidth() {
    return width;
  }

  /**
   * Get height of image.
   *
   * @return Height of image.
   */
  public int getHeight() {
    return height;
  }

  /**
   * Get number of color bands.
   *
   * @return Number of image bands.
   */
  public int getNumBands() {
    return bands;
  }

  /**
   * Adds an metadata field to this header object. The information consists of a
   * key-value pair where the key is an application-specific field name and the 
   * value is the corresponding information for that field.
   * 
   * @param key
   *            the metadata field name
   * @param value
   *            the metadata information
   */
  public void addMetaData(String key, String value) {
    metaData.put(key, value);
  }

  /**
   * Sets the entire metadata map structure.
   *
   * @param metaData hash map containing the metadata key/value pairs
   */
  public void setMetaData(HashMap<String, String> metaData) {
    this.metaData = new HashMap<String, String>(metaData);
  }

  /**
   * Attempt to retrieve metadata value associated with key.
   *
   * @param key field name of the desired metadata record
   * @return either the value corresponding to the key or null if the
   * key was not found
   */
  public String getMetaData(String key) {
    return metaData.get(key);
  }

  /**
   * Get the entire list of all metadata that applications have
   * associated with this image.
   *
   * @return a hash map containing the keys and values of the metadata
   */
  public HashMap<String, String> getAllMetaData() {
    return new HashMap<String, String>(metaData);
  }

  /**
   * Create a binary representation of the application-specific
   * metadata, ready to be serialized into a HIB file.
   *
   * @return A byte array containing the serialized hash map
   */
  public byte[] getMetaDataAsBytes() {
    try {
      String jsonText = JSONValue.toJSONString(metaData);
      final byte[] utf8Bytes = jsonText.getBytes("UTF-8");
      return utf8Bytes;
    } catch (java.io.UnsupportedEncodingException e) {
      System.err.println("UTF-8 encoding exception in getMetaDataAsBytes()");
      return null;
    }
  }

  /**
   * Recreates the general metadata from serialized bytes, usually
   * from the beginning of a HIB file.
   *
   * @param utf8Bytes UTF-8-encoded bytes of a JSON object
   * representing the data
   */
  @SuppressWarnings("unchecked")
  public void setMetaDataFromBytes(byte[] utf8Bytes) {
    try {
      String jsonText = new String(utf8Bytes, "UTF-8");
      JSONObject jsonObject = (JSONObject)JSONValue.parse(jsonText);
      metaData = (HashMap)jsonObject;
    } catch (java.io.UnsupportedEncodingException e) {
      System.err.println("UTF-8 encoding exception in setMetaDataAsBytes()");
    }
  }

  /**
   * Attempt to retrieve EXIF data value for specific key.
   *
   * @param key field name of the desired EXIF data record
   * @return either the value corresponding to the key or null if the
   * key was not found
   */
  public String getExifData(String key) {
    return exifData.get(key);
  }

  /**
   * Get the entire map of EXIF data.
   *
   * @return a hash map containing the keys and values of the metadata
   */
  public HashMap<String, String> getAllExifData() {
    return new HashMap<String, String>(exifData);
  }

  /**
   * Sets the entire EXIF data map structure.
   *
   * @param exifData hash map containing the EXIF data key/value pairs
   */
  public void setExifData(HashMap<String, String> exifData) {
    this.exifData = new HashMap<String, String>(exifData);
  }

  /**
   * Sets the current object to be equal to another
   * ImageHeader. Performs deep copy of meta data.
   *
   * @param header Target image header.
   */
  public void set(HipiImageHeader header) {
    this.storageFormat = header.getStorageFormat();
    this.colorSpace = header.getColorSpace();
    this.width = header.getWidth();
    this.height = header.getHeight();
    this.bands = header.getNumBands();
    this.metaData = header.getAllMetaData();
    this.exifData = header.getAllExifData();
  }

  /**
   * Produce readable string representation of header.
   * @see java.lang.Object#toString
   */
  @Override
  public String toString() {
    String metaText = JSONValue.toJSONString(metaData);
    return String.format("ImageHeader: (%d %d) %d x %d x %d meta: %s", 
			 storageFormat.toInteger(), colorSpace.toInteger(), width, height, bands, metaText);
  }  

  /**
   * Serializes the HipiImageHeader object into a simple uncompressed binary format using the
   * {@link java.io.DataOutput} interface.
   *
   * @see #readFields
   * @see org.apache.hadoop.io.WritableComparable#write
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(storageFormat.toInteger());
    out.writeInt(colorSpace.toInteger());
    out.writeInt(width);
    out.writeInt(height);
    out.writeInt(bands);
    byte[] metaDataBytes = getMetaDataAsBytes();
    if (metaDataBytes == null || metaDataBytes.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(metaDataBytes.length);
      out.write(metaDataBytes);
    }
  }

  /**
   * Deserializes HipiImageHeader object stored in a simple uncompressed binary format using the
   * {@link java.io.DataInput} interface. The first twenty bytes are the image storage type,
   * color space, width, height, and number of color bands (aka channels), all stored as ints,
   * followed by the meta data stored as a set of key/value pairs in JSON UTF-8 format.
   *
   * @see org.apache.hadoop.io.WritableComparable#readFields
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    this.storageFormat = HipiImageFormat.fromInteger(input.readInt());
    this.colorSpace = HipiColorSpace.fromInteger(input.readInt());
    this.width = input.readInt();
    this.height = input.readInt();
    this.bands = input.readInt();
    int len = input.readInt();
    if (len > 0) {
      byte[] metaDataBytes = new byte[len];
      input.readFully(metaDataBytes, 0, len);
      setMetaDataFromBytes(metaDataBytes);
    }
  }

  /**
   * Compare method inherited from the {@link java.lang.Comparable} interface. This method is
   * currently incomplete and uses only the storage format to determine order.
   *
   * @param that another {@link HipiImageHeader} to compare with the current object
   *
   * @return An integer result of the comparison.
   *
   * @see java.lang.Comparable#compareTo
   */
  @Override
  public int compareTo(HipiImageHeader that) {

    int thisFormat = this.storageFormat.toInteger();
    int thatFormat = that.storageFormat.toInteger();

    return (thisFormat < thatFormat ? -1 : (thisFormat == thatFormat ? 0 : 1));
  }

  /**
   * Hash method inherited from the {@link java.lang.Object} base class. This method is
   * currently incomplete and uses only the storage format to determine this hash.
   *
   * @return hash code for this object
   *
   * @see java.lang.Object#hashCode
   */
  @Override
  public int hashCode() {
    return this.storageFormat.toInteger();
  }

}
