package hipi.image;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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
 * The header information for a 2D image. ImageHeader encapsulates
 * universally available information about a 2D image (width, height,
 * and bit depth) along with variable EXIF metadata.<br/><br/>
 *
 * The {@link hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) ImageHeader objects in various
 * image formats such as JPEG and PNG.
 */
public class ImageHeader implements Writable, RawComparator<BinaryComparable> {

  /**
   * Image formats supported in HIPI.
   */
  public enum ImageFormat {
    UNDEFINED(0x0), JPEG(0x1), PNG(0x2), PPM(0x3), RAW_UCHAR_RGB(0x4), RAW_FLOAT_RGB(0x5);

    private int format;

    /**
     * Creates an ImageFormat from an int.
     *
     * @param format Integer representation of ImageFormat.
     */
    ImageFormat(int format) {
      this.format = format;
    }

    /**
     * Creates an ImageFormat from an int.
     *
     * @param val Integer representation of ImageFormat.
     *
     * @return Associated ImageFormat.
     */
    public static ImageFormat fromInteger(int input) throws IllegalArgumentException {
      int format = (input & 0xff);
      for (ImageFormat fmt : values()) {
        if (fmt.format == format) {
          return fmt;
        }
      }
      throw new IllegalArgumentException(String.format("There is no ImageFormat enum value with an associated integer value of %d", input));
    }

    /** 
     * Integer representation of ImageFormat.
     *
     * @return Integer representation of ImageFormat.
     */
    public int toInteger() {
      return format;
    }

    /**
     * Default ImageFormat. Currently UNDEFINED.
     *
     * @return Default ImageFormat enum value.
     */
    public static ImageFormat getDefault() {
      return UNDEFINED;
    }
  }

  /**
   * Color spaces supported in HIPI.
   */
  public enum ColorSpace {
    UNDEFINED(0x0), RGB(0x1), LUM(0x2);

    private int cspace;

    /**
     * Creates a ColorSpace from an int
     *
     * @param format Integer representation of ColorSpace.
     */
    ColorSpace(int cspace) {
      this.cspace = cspace;
    }

    /**
     * Creates a ColorSpace from an int.
     *
     * @param val Integer representation of ColorSpace.
     *
     * @return Associated ColorSpace.
     */
    public static ColorSpace fromInteger(int input) throws IllegalArgumentException {
      int cspace = (input & 0xff);
      for (ColorSpace cs : values()) {
        if (cs.cspace == cspace) {
	  return cs;
        }
      }
      throw new IllegalArgumentException(String.format("There is no ColorSpace enum value with an associated integer value of %d", input));
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
     * Default ColorSpace. Currently RGB.
     *
     * @return Default ColorSpace enum value.
     */
    public static ImageFormat getDefault() {
      return RGB;
    }

  }

  private ImageType storageType;     // format used to store image on HDFS
  private ColorSpace colorSpace;     // color space of pixel data
  private int width;                 // width of image
  private int height;                // height of image
  private int bands;                 // number of color bands (aka channels)

  /**
   * A map containing key/value pairs of meta/EXIF data associated
   * with this image.
   */
  private Map<String, String> metaData = new HashMap<String,String>();

  /**
   * Creates an ImageHeader.
   */
  public ImageHeader(ImageType storageType, ColorSpace colorSpace, 
		     int width, int height,
		     int bands, byte[] metaDataBytes)
    throws IllegalArgumentException {
    if (width < 1 || height < 1 || bands < 1) {
      throw new IllegalArgumentException(String.format("Invalid spatial dimensions or number of bands: (%d,%d,%d)", width, height, bands));
    }
    this.storageType = storageType;
    this.colorSpace = colorSpace;
    this.width = width;
    this.height = height;
    this.bands = bands;
    if (metaDataBytes != null) {
      setMetaDataFromBytes(metaDataBytes);
    }
  }

  /**
   * Get the image storage type.
   *
   * @return Current image storage type.
   */
  public ImageType getStorageType() {
    return storageType;
  }

  /**
   * Get the image color space.
   *
   * @return Image color space.
   */
  public ColorSpace getColorSpace() {
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
   * Adds a new map full of meta data, similar to addMetaData(String, String),
   * but iterating through the input list overwriting any existing values.
   *
   * @param input
   *            the map containing the new meta data
   */
  public void addMetaData(Map<String, String> input) {
    for (Map.Entry<String, String> entry : input.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      addMetaData(key, value);
    }
  }

  /**
   * Attempt to retrieve metadata value associated with key.
   *
   * @param key field name of the desired metadata record
   * @return either the value corresponding to the key or the empty
   *         string if the key was not found
   */
  public String getMetaData(String key) {
    String value = metaData.get(key);
    if (value == null) {
      return "";
    } else {
      return value;
    }
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
   * Sets the current object to be equal to another
   * ImageHeader. Performs deep copy of meta data.
   *
   * @param header Target image header.
   */
  public void set(ImageHeader header) {
    this.storageType = header.getStorageType();
    this.colorSpace = header.getColorSpace();
    this.width = header.getWidth();
    this.height = header.getHeight();
    this.bands = header.getNumBands();
    this.metaData = header.getAllMetaData();
  }

  /**
   * Produces a string representation of the image header.
   *
   * @return String representation of image header.
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(storageType.toInteger() + " " +
		  colorSpace.toInteger() + " " +
		  width + " " +
		  height + " " +
		  bands);
    return result.toString();
  }  

  /**
   * Compare method from the {@link RawComparator} interface.
   *
   * @return An integer result of the comparison.
   */
  public int compare(byte[] byteArray1, int start1, int length1, 
		     byte[] byteArray2, int start2, int length2) {

    int st1 = ByteUtils.ByteArrayToInt(byteArray1, start1);
    int st2 = ByteUtils.ByteArrayToInt(byteArray2, start2);

    int cs1 = ByteUtils.ByteArrayToInt(byteArray1, start1 + 4);
    int cs2 = ByteUtils.ByteArrayToInt(byteArray2, start2 + 4);

    int w1  = ByteUtils.ByteArrayToInt(byteArray1, start1 +  8);
    int w2  = ByteUtils.ByteArrayToInt(byteArray2, start2 +  8);

    int h1  = ByteUtils.ByteArrayToInt(byteArray1, start1 + 12);
    int h2  = ByteUtils.ByteArrayToInt(byteArray2, start2 + 12);

    int b1  = ByteUtils.ByteArrayToInt(byteArray1, start1 + 16);
    int b2  = ByteUtils.ByteArrayToInt(byteArray2, start2 + 16);

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
   * Reads an ImageHeader stored in a simple uncompressed binary
   * format. The first twenty bytes are the image type, width, height,
   * bit depth, and number of color bands (aka channels), all stored
   * as ints, followed by the meta data stored as a set of key/value
   * pairs.
   *
   * @param input Interface for reading bytes from a binary stream.
   * @throws IOException
   */
  public void readFields(DataInput input) throws IOException {
    this.storageType = ImageType.fromValue(input.readInt());
    this.colorSpace = ColorSpace.fromValue(input.readInt());
    this.width = input.readInt();
    this.height = input.readInt();
    this.bands = input.readInt();
    int len = in.readInt();
    if (len > 0) {
      byte[] metaDataBytes = new byte[len];
      input.readFully(metaDataBytes, 0, len);
      setMetaDataFromBytes(metaDataBytes);
    }
  }

  /**
   * Writes ImageHeader in a simple uncompressed binary format.
   *
   * @param output Interface for writing bytes to a binary stream.
   * @throws IOException
   * @see #readFields
   */
  public void write(DataOutput output) throws IOException {
    output.writeInt(storageType.toValue());
    output.writeInt(colorSpace.toValue());
    output.writeInt(width);
    output.writeInt(height);
    output.writeInt(bands);
    out.writeInt(metaData.size());
    byte[] metaDataBytes = getMetaDataAsBytes();
    if (metaDataBytes == null || metaDataBytes.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(metaDataBytes.length);
      out.write(metaDataBytes);
    }
  }
}
