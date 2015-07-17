package hipi.image;

import hipi.image.PixelArray;
import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiColorSpace;
import hipi.image.HipiImage;
import hipi.image.PixelArray;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.IllegalArgumentException;

/**
 * A raster (uncompressed) 2D image in HIPI. A raster image consists
 * of an ImageHeader (inherited from the HipiImage abstract base
 * class) and a flat array of uncompressed image pixel data stored in
 * interleaved raster-scan order (e.g., RGBRGBRGB...). The underlying
 * type used to represent the pixel data is set using the
 * @{link RasterImage#setPixelArray} method.
 */
public abstract class RasterImage extends HipiImage {

  protected PixelArray pixelArray;

  protected RasterImage(PixelArray pixelArray) {
    this.pixelArray = pixelArray;
  }

  public void setHeader(HipiImageHeader header) throws IllegalArgumentException {
    super.setHeader(header);
    int size = header.getWidth()*header.getHeight()*header.getNumBands();
    pixelArray.setSize(size);
  }

  public PixelArray getPixelArray() {
    return pixelArray;
  }

  /**
   * Compares two RasterImage objects for equality allowing for some
   * amount of differences in pixel values.
   *
   * @return True if the two images have equal dimensions, color
   * spaces, and are found to deviate by less than a specified maximum
   * difference, false otherwise.
   */
  public abstract boolean equalsWithTolerance(RasterImage thatImage, float maxDifference);

  /**
   * Compares two RasterImage objects for equality.
   *
   * @return True if the two images are found to deviate by an amount
   * that is not representable in the underlying pixel type, false
   * otherwise.
   */
  @Override
  public abstract boolean equals(Object that);

  /**
   * Crops a raster image to a (width x height) rectangular region
   * with top-left corner at (x,y) pixel location. Note that last
   * argument is output target.
   * 
   * @return a {@link RasterImage} containing the cropped portion of
   * the original image
   */
  public void crop(int x, int y, int width, int height, RasterImage crop) throws IllegalArgumentException {
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();

    // Verify crop dimensions
    if (x < 0 || width <= 0 || x+width > w ||
	y < 0 || height <= 0 || y+height > h) {
      throw new IllegalArgumentException("Invalid crop region.");
    }

    // Verify crop output target
    if (width != crop.getWidth() || height != crop.getHeight() || b != crop.getNumBands()) {
      throw new IllegalArgumentException("Mismatch between size of crop region and size of crop output target.");
    }

    PixelArray pixelArrayCrop = crop.getPixelArray();

    // Assemble cropped output
    for (int j=y; j<y+height; j++) {
      for (int i=x; i<x+width; i++) {
	for (int c=0; c<b; c++) {
	  pixelArrayCrop.setElem(((j-y)*width+(i-x))*b+c,pixelArray.getElem((j*w+i)*b+c));
	}
      }
    }    
  }

  /**
   * Convert image to another color space.
   *
   * @param colorSpace Indicates target color space.
   * 
   * @return A {@link RasterImage} of the converted image. Returns
   * null if the conversion could not be performed.
   */
  public RasterImage convertToColorSpace(HipiColorSpace colorSpace) throws IllegalArgumentException {
    if (header.getColorSpace() == colorSpace) {
      throw new IllegalArgumentException("Cannot convert color space to itself.");
    }
    throw new IllegalArgumentException("Not implemented.");
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
    String typeString = "UNDEFINED IMAGE TYPE";
    switch (getType()) {
    case FLOAT:
      typeString = "FloatImage";
      break;
    case BYTE:
      typeString = "ByteImage";
      break;
    default:
    }
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    StringBuilder result = new StringBuilder();
    result.append(String.format("%s: %d x %d x %d [", typeString, w, h, b));
    int n = Math.min(10,w*h);
    for (int i=0; i<n; i++) {
      result.append("(");
      for (int c=0; c<b; c++) {
	if (getType() == HipiImageType.FLOAT) {
	  result.append(String.format("%.2f",pixelArray.getElemFloat(i*b+c)));
	} else {
	  result.append(pixelArray.getElem(i*b+c));
	}
	if (c<(b-1))
	  result.append(" ");
	else
	  result.append(")");
      }
      if (i<(n-1)) {
	result.append(" ");
      }
    }
    result.append("]");
    return result.toString();
  }

  /**
   * Sets the current object to be equal to another
   * RasterImage. Performs a shallow copy of the image header and
   * pixel data array.
   *
   * @param image Target image.
   */
  public void set(RasterImage image) {
    this.header = image.header;
    this.pixelArray = image.pixelArray;
  }

  /**
   * Writes raster image in a simple uncompressed binary format.
   * @see org.apache.hadoop.io.Writable#write
   */
  @Override
  public void write(DataOutput output) throws IOException {
    header.write(output);
    output.write(pixelArray.getByteArray());
  }

  /**
   * Reads a raster image stored in a simple uncompressed binary
   * format.
   * @see org.apache.hadoop.io.Writable#readFields
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    // Create and read header
    header = new HipiImageHeader(input);
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    int numBytes = w*h*b*PixelArray.getDataTypeSize(pixelArray.getDataType());
    // Read pixel data
    byte[] pixelBytes = new byte[numBytes];
    input.readFully(pixelBytes);
    pixelArray.setFromByteArray(pixelBytes);
  }

} // public abstract class RasterImage<T>...
