package hipi.image;

import hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * Base class for FloatImage's. A FloatImage is just an array of floating-point
 * values along with information about the dimensions of the image that array
 * represents. The is the default image type that is used in HIPI.
 * 
 * You can convert to other image types using {@link ImageConverter}.
 * 
 * @see ImageConverter, UCharImage, GenericImage, DoubleImage
 * 
 * @author seanarietta
 * 
 */
public class FloatImage implements Writable, RawComparator<BinaryComparable> {

	protected int _w;
	protected int _h;
	protected int _b;
	protected float[] _pels;

	protected FloatImage(int width, int height, int bands) {
		_w = width;
		_h = height;
		_b = bands;
	}

	/**
	 * Get a pixel from this image and check the bounds while doing it
	 * 
	 * @param x
	 * @param y
	 * @param c
	 * @return Either the requested pixel value or -Float.MAX_VALUE if the pixel
	 *         is out of bounds
	 */
	public float getPixelCheckBounds(int x, int y, int c) {
		if (x >= 0 && x < _w && y >= 0 && y < _h && c >= 0 && c < _b) {
			return _pels[c + (x + y * _w) * _b];
		} else {
			return -Float.MAX_VALUE;
		}
	}

	public float getPixel(int x, int y, int c) {
		return _pels[c + (x + y * _w) * _b];
	}

	public void readFields(DataInput input) throws IOException {
		_w = input.readInt();
		_h = input.readInt();
		_b = input.readInt();
		byte[] pixel_buffer = new byte[_w * _h * _b * 4];
		input.readFully(pixel_buffer);
		_pels = ByteUtils.ByteArraytoFloatArray(pixel_buffer);
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(_w);
		output.writeInt(_h);
		output.writeInt(_b);
		output.write(ByteUtils.FloatArraytoByteArray(_pels));
	}

	/**
	 * This method comes from the RawComparator class and allows sorting to
	 * happen much faster than in the normal Comparable interface. For a
	 * discussion of this, see Hadoop: The Definitive Guide. Essentially, this
	 * method avoids deserializing the entire FloatImage object before doing a
	 * comparison. Since the first bytes indicate the size of the image, we can
	 * just read a small segment of the byte array to get the sizes.
	 * 
	 * TODO: Ensure that the second and fifth parameters are actually defining
	 * the start
	 */
	public int compare(byte[] byte_array1, int start1, int length1,
			byte[] byte_array2, int start2, int length2) {
		int w1 = ByteUtils.ByteArrayToInt(byte_array1, start1);
		int w2 = ByteUtils.ByteArrayToInt(byte_array2, start2);

		int h1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 4);
		int h2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 4);

		int b1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 8);
		int b2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 8);

		int size1 = w1 * h1 * b1;
		int size2 = w2 * h2 * b2;

		return (size1 - size2);
	}

	/**
	 * TODO: Ensure that this method is working appropriately. I'm not sure that
	 * the getBytes() function returns the bytes that I think it does. Also, why
	 * does this function even get called. Shouldn't the method above be the
	 * only one that is called ever?
	 */
	public int compare(BinaryComparable o1, BinaryComparable o2) {
		byte[] b1 = o1.getBytes();
		byte[] b2 = o2.getBytes();
		int length1 = o1.getLength();
		int length2 = o2.getLength();

		return compare(b1, 0, length1, b2, 0, length2);
	}
}
