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

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Base class for ImageHeaders. An ImageHeader is a way to store header and 
 * metadata of images, including EXIF information.
 *  
 * 
 */
public class ImageHeader implements Writable, RawComparator<BinaryComparable> {

	public int width;
	public int height;
	public int bitDepth;
	
	public enum ImageType {
		UNSUPPORTED_IMAGE(0x0),
		JPEG_IMAGE(0x1), 
		PNG_IMAGE(0x2), 
		PPM_IMAGE(0x3);

		private int _val;
		ImageType(int val) {
			_val = val;
		}

		public static ImageType fromValue(int input) {
      int value = (input & 0xff);
			for (ImageType type : values()) {
				if (type._val == value) {
					return type;
				}
			}
			return getDefault();
		}

		public int toValue() {
			return _val;
		}

		public static ImageType getDefault() {
			return UNSUPPORTED_IMAGE;
		}
	}

	/**
	 * A Map containing the key-value pairs where the key is the field name as
	 * it appears in the EXIF 2.2 specification and the value is the
	 * corresponding information for that field.
	 */
	private Map<String, String> _exif_information = new HashMap<String, String>();
	/**
	 * The image type of this image. Usually read from the image file's first
	 * few bytes.
	 */
	private ImageType _image_type;
	/**
	 * A Map containing the key-value pairs of general meta-data for the image.
	 * This is stored adjacent to the image in the HIB file, but unlike EXIF is
   * not stored within the image, and can be arbitrary key/value string pairs.
	 */
	private Map<String, String> _meta_data = new HashMap<String, String>();

	/**
	 * Adds an EXIF field to this header object. The information consists of a
	 * key-value pair where the key is the field name as it appears in the EXIF
	 * 2.2 specification and the value is the corresponding information for that
	 * field.
	 * 
	 * @param key
	 *            the field name of the EXIF information
	 * @param value
	 *            the EXIF information
	 */
	public void addEXIFInformation(String key, String value) {
		_exif_information.put(key, value);
	}

	/**
	 * Get an EXIF value designated by the key. The key should correspond to the
	 * 'Field Name' in the EXIF 2.2 specification.
	 * 
	 * @param key
	 *            the field name of the EXIF information desired
	 * @return either the value corresponding to the key or the empty string if
	 *         the key was not found
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
		_meta_data.put(key, value);
	}

	/**
	 * Get an metadata value designated by the key. The key is an arbitrary
   * application-defined string.
	 *
	 * @param key
	 *            the field name of the metadata information desired
	 * @return either the value corresponding to the key or the empty string if
	 *         the key was not found
	 */
	public String getMetaData(String key) {
		String value = _meta_data.get(key);

		if (value == null) {
			return "";
		} else {
			return value;
		}
	}

  /**
   * Create a binary representation of the application-specific metadata, ready
   * to be serialized into a HIB file.
   *
   * @return A byte array containing the JSON serialized metadata, encoded as UTF-8
   */
  public byte[] getMetaDataAsBytes() {
    try {
      String jsonText = JSONValue.toJSONString(_meta_data);
      final byte[] utf8Bytes = jsonText.getBytes("UTF-8");
      return utf8Bytes;
    } catch (java.io.UnsupportedEncodingException e) {
      System.err.println("UTF-8 encoding exception in getMetaDataAsBytes()");
      return null;
    }
  }

  /**
   * Recreates the general metadata from serialized bytes, usually from a HIB file.
   *
   * @param utf8Bytes
   *                The UTF-8-encoded bytes of a JSON object representing the data
   */
  public void setMetaDataFromBytes(byte[] utf8Bytes) {
    try {
      String jsonText = new String(utf8Bytes, "UTF-8");
      JSONObject jsonObject = (JSONObject)JSONValue.parse(jsonText);
      _meta_data = (HashMap)jsonObject;
    } catch (java.io.UnsupportedEncodingException e) {
      System.err.println("UTF-8 encoding exception in setMetaDataAsBytes()");
    }
  }

	public ImageHeader(ImageType type, byte[] meta_data_bytes)
	{
		_image_type = type;
    if (meta_data_bytes != null) {
      setMetaDataFromBytes(meta_data_bytes);
    }
	}
	
	public ImageHeader()
	{
		_image_type = ImageType.getDefault();
	}

	/**
	 * Get the image type.
	 */
	public ImageType getImageType() {
		return _image_type;
	}

	public int compare(BinaryComparable o1, BinaryComparable o2) {
		return 0;
	}

	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
			int arg5) {
		return 0;
	}

	public void readFields(DataInput in) throws IOException {
		bitDepth = in.readInt();
		height = in.readInt();
		width = in.readInt();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			String key = Text.readString(in);
			String value = Text.readString(in);
			_exif_information.put(key, value);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(bitDepth);
		out.writeInt(height);
		out.writeInt(width);
		out.writeInt(_exif_information.size());
		Iterator<Entry<String, String>> it = _exif_information.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			Text.writeString(out, entry.getKey());
			Text.writeString(out, entry.getValue());
		}
	}
}
