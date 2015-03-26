package hipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

  public int width;
  public int height;
  public int bitDepth;

  /**
   * Image types supported in HIPI.
   */
  public enum ImageType {
    UNSUPPORTED_IMAGE(0x0), JPEG_IMAGE(0x1), PNG_IMAGE(0x2), PPM_IMAGE(0x3);

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
     * Default ImageType. Currently UNSUPPORTED_IMAGE.
     *
     * @return Default ImageType enum value.
     */
    public static ImageType getDefault() {
      return UNSUPPORTED_IMAGE;
    }
  }

  /**
   * Private map object for EXIF image metadata.
   */
  private Map<String, String> _exif_information = new HashMap<String, String>();

  /**
   * Private field that stores type of image. Usually determined from
   * the first few bytes of image file.
   */
  private ImageType _image_type;

  /**
   * Adds an EXIF key/value pair to the ImageHeader object. The key
   * corresponds to the "field name" in the <a target="_blank"
   * href="http://www.kodak.com/global/plugins/acrobat/en/service/digCam/exifStandard2.pdf">EXIF
   * 2.2 specification</a>.
   * 
   * @param key Key or "field name" of the EXIF record.
   * @param value Value of the EXIF record.
   */
  public void addEXIFInformation(String key, String value) {
    _exif_information.put(key, value);
  }

  /**
   * Get EXIF value associated with key, if present.
   * 
   * @param key Key or "field name" of the desired EXIF record.
   *
   * @return Value corresponding to key if present or the empty string if not.
   *
   * @see #addEXIFInformation
   */
  public String getEXIFInformation(String key) {
    String value = _exif_information.get(key);

    if (value == null) {
      return "";
    } else {
      return value;
    }
  }

  /**
   * Creates an ImageHeader initialized with type.
   *
   * @param type ImageType of new ImageHeader.
   */
  public ImageHeader(ImageType type) {
    _image_type = type;
  }

  /**
   * Creates an ImageHeader initialized with the default type.
   *
   */
  public ImageHeader() {
    _image_type = ImageType.getDefault();
  }

  /**
   * Get the image type.
   *
   * @return Current image type.
   */
  public ImageType getImageType() {
    return _image_type;
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
  public void readFields(DataInput input) throws IOException {
    bitDepth = input.readInt();
    height = input.readInt();
    width = input.readInt();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      String key = Text.readString(input);
      String value = Text.readString(input);
      _exif_information.put(key, value);
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
    output.writeInt(bitDepth);
    output.writeInt(height);
    output.writeInt(width);
    output.writeInt(_exif_information.size());
    Iterator<Entry<String, String>> it = _exif_information.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, String> entry = it.next();
      Text.writeString(output, entry.getKey());
      Text.writeString(output, entry.getValue());
    }
  }

  /**
   * Get width of image.
   *
   * @return Width of image.
   */
  public int getWidth() {
    return this.width;
  }

  /**
   * Get height of image.
   *
   * @return Height of image.
   */
  public int getHeight() {
    return this.height;
  }

  /**
   * Get map containing EXIF metadata key/value records.
   *
   * @return Map of EXIF records.
   */
  public Map<String, String> getEXIFInformation() {
    return this._exif_information;
  }

  /**
   * Sets the current object to be equal to another
   * ImageHeader. Performs shallow copy of EXIF record hash map.
   *
   * @param header Target image header.
   */
  public void set(ImageHeader header) {
    this.width = header.getWidth();
    this.height = header.getHeight();
    this._image_type = header.getImageType();
    this._exif_information = header.getEXIFInformation();
  }
}
