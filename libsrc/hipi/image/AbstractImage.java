package hipi.image;

import hipi.image.ImageHeader;
import hipi.image.HipiImageException;
import hipi.util.ByteUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

/**
 * Abstract base class representing a 2D image in HIPI. An image
 * consists of an ImageHeader and a representation of the pixel data
 * which may be compressed, encoded, both, or neither.
 */
public class AbstractImage implements Writable, RawComparator<BinaryComparable> {

  protected ImageHeader header;

  public AbstractImage convert(ImageHeader.ImageType newtype) throws HipiImageException;

}
