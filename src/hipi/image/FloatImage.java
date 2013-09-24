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
 * You can convert to other image types using ImageConverter.
 * 
 * @see hipi.image.convert.ImageConverter
 * 
 */
public class FloatImage implements Writable, RawComparator<BinaryComparable> {

	private int _w;
	private int _h;
	private int _b;
	private float[] _pels;

	public FloatImage() {}

	public FloatImage(int width, int height, int bands, float[] pels) {
		_w = width;
		_h = height;
		_b = bands;
		_pels = pels;
	}

	public FloatImage(int width, int height, int bands) {
		this(width, height, bands, new float[width * height * bands]);
	}

	public FloatImage deepCopy() {
		return new FloatImage(getWidth(),
				getHeight(),
				getBands(),
				_pels.clone());
	}
	
	// define kernel here (double loop), these are the 1/16, 2/16, etc...
	// values that you're multiplying the image pixels by
	public FloatImage convolve(float[][] kernel) {
		int kernel_rows = kernel.length;
		int kernel_cols = kernel[0].length;

		// iterate over each pixel in the image
		// leave a kernel_rows/2 sized gap around the edge of the image
		// so that we don't run into IndexOutOfBounds exceptions
		// when performing the convolution
		FloatImage convolved_image = this.deepCopy();
		for (int row = kernel_rows/2; row < _h - (kernel_rows - kernel_rows/2); row++) {
			for (int col = kernel_cols/2; col < _w - (kernel_rows - kernel_cols/2); col++) {

				float[] band_pix = new float[_b];
				// iterate over each pixel in the kernel
				for (int row_offset = 0 ; row_offset < kernel_rows; row_offset++ ) {
					for (int col_offset = 0 ; col_offset < kernel_cols; col_offset++ ) {
						// subtract by half the kernel size to center the kernel
						// on the pixel in question
						int row_index = row + row_offset - kernel_rows/2;
						int col_index = col + col_offset - kernel_cols/2;
						for (int band = 0; band < _b; band++) {
							band_pix[band] += this.getPixel(col_index, row_index, band) * kernel[row_offset][col_offset];
						}
					}
				}
				for (int band = 0; band < _b; band++) {
					convolved_image.setPixel(col, row, band, band_pix[band]);
				}
			}
		}
		return convolved_image;
	}

	public FloatImage downsample(int factor) {
		// Create 5x5 gaussian kernel
		float[][] gauss_kernel = new float[][]{{1, 4, 6, 4, 1},
				{4, 16, 24, 16, 4}, 
				{6, 24, 36, 24, 6}, 
				{4, 16, 24, 16, 4}, 
				{1, 4, 6, 4, 1}};
		for (int i = 0; i < 5; i++){
			for (int j = 0; j < 5; j++){
				gauss_kernel[i][j] /= (256.0);
			}
		}

		FloatImage downsampled_image = this.deepCopy();
		for (int f = 0; f < factor; f++) {
			// Blur first, then half-sample
			FloatImage blurred_image = downsampled_image.convolve(gauss_kernel);
			downsampled_image = new FloatImage(blurred_image.getWidth()/2, blurred_image.getHeight()/2, _b);
			// Half-sample
			for (int i = 0; i < downsampled_image.getWidth(); i++) {
				for (int j = 0; j < downsampled_image.getHeight(); j++) {
					for (int k = 0; k < downsampled_image.getBands(); k++)
						downsampled_image.setPixel(i, j, k, blurred_image.getPixel(2*i, 2*j, k));
				}
			}
		}
		return downsampled_image;
	}
	
	public FloatImage upsample(int factor) {
		// Create 5x5 gaussian kernel
		float[][] gauss_kernel = new float[][]{{1, 4, 6, 4, 1},
				{4, 16, 24, 16, 4}, 
				{6, 24, 36, 24, 6}, 
				{4, 16, 24, 16, 4}, 
				{1, 4, 6, 4, 1}};
		for (int i = 0; i < 5; i++){
			for (int j = 0; j < 5; j++){
				gauss_kernel[i][j] /= (64.0);
			}
		}

		FloatImage upsampled_image = this.deepCopy();
		for (int f = 0; f < factor; f++) {
			// Blur first, then half-sample
			FloatImage buffered_image = upsampled_image.deepCopy();
			upsampled_image = new FloatImage(buffered_image.getWidth()*2, buffered_image.getHeight()*2, _b);
			// Half-sample
			for (int i = 0; i < buffered_image.getWidth(); i++) {
				for (int j = 0; j < buffered_image.getHeight(); j++) {
					for (int k = 0; k < buffered_image.getBands(); k++)
						upsampled_image.setPixel(2*i, 2*j, k, buffered_image.getPixel(i, j, k));
				}
			}
			upsampled_image = upsampled_image.convolve(gauss_kernel);
		}
		return upsampled_image;
	}
	/**
	 * Crops a float image according the the x,y location and the width, height passed in.
	 * 
	 * @return a {@link FloatImage} containing the cropped portion of the original image
	 */
	public FloatImage crop(int x, int y, int width, int height) {
		float[] pels = new float[width * height * _b];
		for (int i = y; i < y + height; i++)
			for (int j = x * _b; j < (x + width) * _b; j++)
				pels[(i - y) * width * _b + j - x * _b] = _pels[i * _w * _b + j];
		return new FloatImage(width, height, _b, pels);
	}

	public static final int RGB2GRAY = 0x01;

	/**
	 * Convert between color types (black and white, grayscale, etc.). Currently only RGB2GRAY
	 * 
	 * @return A {@link FloatImage} of the converted image. Returns null if the image could not be converted
	 */
	public FloatImage convert(int type) {
		switch (type) {
		case RGB2GRAY:
			float[] pels = new float[_w * _h];
			for (int i = 0; i < _w * _h; i++)
				pels[i] = _pels[i * _b] * 0.30f + _pels[i * _b + 1] * 0.59f + _pels[i * _b + 2] * 0.11f;
			return new FloatImage(_w, _h, 1, pels);
		}
		return null;
	}

	// Assumes there are the same number of bands in both imgs.
	public FloatImage concatenate(FloatImage new_img) {
		FloatImage concat_image = new FloatImage(_w + new_img.getWidth(), _h + new_img.getHeight(), _b);
		// Copy first image
		for (int i = 0; i < this.getWidth(); i++) {
			for (int j = 0; j < this.getHeight(); j++){
				for (int b = 0; b < this.getBands(); b++)
					concat_image.setPixel(i, j, b, this.getPixel(i, j, b));
			}
		}
		// Copy second image
		for (int i = 0; i < new_img.getWidth(); i++) {
			for (int j = 0; j < new_img.getHeight(); j++){
				for (int b = 0; b < new_img.getBands(); b++)
					concat_image.setPixel(i + this.getWidth(), j, b, new_img.getPixel(i, j, b));
			}
		}
		return concat_image;
	}
	
	/**
	 * Adds a {@link FloatImage} to the current image
	 * 
	 * @param image
	 */
	public void add(FloatImage image) {
		float[] pels = image.getData();
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] += pels[i];
	}
	/**
	 * Adds a scalar to every pixel in the FloatImage
	 * 
	 * @param number
	 */
	public void add(float number) {
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] += number;
	}
	
	/**
	 * Subtracts a {@link FloatImage} to the current image
	 * 
	 * @param image
	 */
	public void subtract(FloatImage image) {
		float[] pels = image.getData();
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] -= pels[i];
	}
	/**
	 * Subtracts a scalar to every pixel in the FloatImage
	 * 
	 * @param number
	 */
	public void subtract(float number) {
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] -= number;
	}
	/**
	 * 
	 * @param image Each value is scaled by the corresponding value in image
	 */
	public void scale(FloatImage image) {
		float[] pels = image.getData();
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] *= pels[i];
	}

	public void scale(float number) {
		for (int i = 0; i < _w * _h * _b; i++)
			_pels[i] *= number;
	}

	public float getPixel(int x, int y, int c) {
		return _pels[c + (x + y * _w) * _b];
	}

	public void setPixel(int x, int y, int c, float val) {
		_pels[c + (x + y * _w) * _b] = val;
	}

	public int getWidth() {
		return _w;
	}

	public int getHeight() {
		return _h;
	}

	public int getBands() {
		return _b;
	}

	public float[] getData() {
		return _pels;
	}

	public String hex() {
		return ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(_pels));
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

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(_w + " " + _h + " " + _b + "\n");
		for (int i = 0; i < _h; i++) {
			for (int j = 0; j < _w * _b; j++) {
				result.append(_pels[i * _w * _b + j]);
				if (j < _w * _b - 1)
					result.append(" ");
			}
			result.append("\n");
		}
		return result.toString();
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

		System.out.println("here in the compare");

		return (size1 - size2);
	}

	public int compare(BinaryComparable o1, BinaryComparable o2) {
		byte[] b1 = o1.getBytes();
		byte[] b2 = o2.getBytes();
		int length1 = o1.getLength();
		int length2 = o2.getLength();

		return compare(b1, 0, length1, b2, 0, length2);
	}
}
