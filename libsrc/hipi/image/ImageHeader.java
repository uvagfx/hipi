package hipi.image;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;

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
   * Image types supported in HIPI.
   */
  public enum ImageType {
    UNSUPPORTED_IMAGE(0x0), JPEG_IMAGE(0x1), PNG_IMAGE(0x2), UCHAR_IMAGE(0x3), FLOAT_IMAGE(0x4);

    private int _val;

    /**
     * Creates an ImageType from an int.
     *
     * @param val Integer representation of ImageType.
     */
    ImageType(int val) {
      _val = val;
    }

    /**
     * Creates an ImageType from an int.
     *
     * @param val Integer representation of ImageType.
     *
     * @return Associated ImageType.
     */
    public static ImageType fromValue(int val) {
      for (ImageType type : values()) {
        if (type._val == val) {
          return type;
        }
      }
      return getDefault();
    }

    /** 
     * Integer representation of ImageType.
     *
     * @return Integer representation of ImageType.
     */
    public int toValue() {
      return _val;
    }

    /** 
     * Indicates whether or not image is decoded and ready to access
     * pixel data.
     *
     * @return true if image has been decoded, false otherwise
     */
    public boolean isDecoded() {
      switch (_val) {
      case UCHAR_IMAGE:
      case FLOAT_IMAGE:
	return true;
      case JPEG_IMAGE:
      case PNG_IMAGE:
      case UNSUPPORTED_IMAGE:
      default:
	break;
      }
      return false;
    }

    /**
     * Default ImageType. Currently UNSUPPORTED_IMAGE.
     *
     * @return Default ImageType enum value.
     */
    public static ImageType getDefault() {
      return UNSUPPORTED_IMAGE;
    }
  }

  private ImageType imageType;
  private int width;          // width of image
  private int height;         // height of image
  private int bitDepth;       // bits per channel
  private int numChannels;    // color channels / planes
  private JSONObject records; // metadata stored as collection of key/value pairs

  /**
   * Creates an ImageHeader.
   */
  public ImageHeader(ImageType _imageType, int _width, int _height, 
		     int _bitDepth, int _numChannels) 
    throws IllegalArgumentException {
    if (_width < 0 || _height < 0 || _bitDepth < 1 || _numChannels < 1) {
      throw new IllegalArgumentException(String.format("Invalid dimensions or bit depth or num channels: (%d,%d,%d,%d)", _width, _height, _bitDepth, _numChannels));
    }
    imageType = _imageType;
    width = _width;
    height = _height;
    bitDepth = _bitDepth;
    numChannels = _numChannels;
  }

  /**
   * Get the image type.
   *
   * @return Current image type.
   */
  public ImageType getImageType() {
    return imageType;
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
   * Get bit depth of image.
   *
   * @return Bit depth of image.
   */
  public int getBitDepth() {
    return bitDepth;
  }

  /**
   * Get number of color channels.
   *
   * @return Number of channels.
   */
  public int getNumChannels() {
    return numChannels;
  }

  /**
   * Get JSONObject with collection of image metadata key/value pairs.
   *
   * @return JSONObject with metadata key/value pairs.
   */
  public JSONObject getRecords() {
    return records;
  }

  /**
   * Sets the current object to be equal to another
   * ImageHeader. Performs shallow copy of EXIF record hash map.
   *
   * @param header Target image header.
   */
  public void set(ImageHeader header) {
    imageType = header.getImageType();
    width = header.getWidth();
    height = header.getHeight();
    bitDepth = header.getBitDepth();
    numChannels = header.getNumChannels();
    records = header.getRecords();
  }

  /**
   * Compare method from the {@link java.util.Comparator}
   * interface. Currently unimplemented and always returns zero.
   *
   * @return An integer result of the comparison. Currently always zero.
   */
  public int compare(BinaryComparable o1, BinaryComparable o2) {
    return 0;
  }

  /**
   * Compare method from the {@link RawComparator}
   * interface. Currently unimplemented and always returns zero.
   *
   * @return An integer result of the comparison. Currently always zero.
   */
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
    return 0;
  }

  /**
   * Reads an ImageHeader stored in a simple uncompressed binary
   * format. The first four bytes are the bit depth, width, and
   * height, and count of EXIF records, followed by the EXIF key/value
   * records stored as strings.
   *
   * @param input Interface for reading bytes from a binary stream.
   * @throws IOException
   */
  public void readFields(DataInput input) throws IOException, JSONException {
    imageType = ImageType.fromValue(input.readInt());
    width = input.readInt();
    height = input.readInt();
    bitDepth = input.readInt();
    numChannels = input.readInt();
    records = new JSONObject(new JSONTokener(input));
  }

  /**
   * Writes ImageHeader in a simple uncompressed binary format.
   *
   * @param output Interface for writing bytes to a binary stream.
   * @throws IOException
   * @see #readFields
   */
  public void write(DataOutput output) throws IOException, JSONException {
    output.writeInt(imageType.toValue());
    output.writeInt(width);
    output.writeInt(height);
    output.writeInt(bitDepth);
    output.writeInt(numChannels);
    output.writeString(records.toString());
  }
}
