package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.RasterImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.PixelArray;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.HashMap;

import javax.imageio.metadata.IIOMetadata;
import javax.imageio.ImageIO;

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 1997-2007 Sun Microsystems, Inc. All rights reserved.
 * 
 * The contents of this file are subject to the terms of either the GNU General Public License
 * Version 2 only ("GPL") or the Common Development and Distribution License("CDDL") (collectively,
 * the "License"). You may not use this file except in compliance with the License. You can obtain a
 * copy of the License at http://www.netbeans.org/cddl-gplv2.html or nbbuild/licenses/CDDL-GPL-2-CP.
 * See the License for the specific language governing permissions and limitations under the
 * License. When distributing the software, include this License Header Notice in each file and
 * include the License file at nbbuild/licenses/CDDL-GPL-2-CP. Sun designates this particular file
 * as subject to the "Classpath" exception as provided by Sun in the GPL Version 2 section of the
 * License file that accompanied this code. If applicable, add the following below the License
 * Header, with the fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * 
 * Contributor(s): Alexandre Iline.
 * 
 * The Original Software is the Jemmy library. The Initial Developer of the Original Software is
 * Alexandre Iline. All Rights Reserved.
 * 
 * If you wish your version of this file to be governed by only the CDDL or only the GPL Version 2,
 * indicate your decision by adding "[Contributor] elects to include this software in this
 * distribution under the [CDDL or GPL Version 2] license." If you do not indicate a single choice
 * of license, a recipient has the option to distribute your version of this file under either the
 * CDDL, the GPL Version 2 or to extend the choice of license to its licensees as provided above.
 * However, if you add GPL Version 2 code and therefore, elected the GPL Version 2 license, then the
 * option applies only if the new code is made subject to such option by the copyright holder.
 * 
 * Heavy modifications were made to the original library by Chris Sweeney.
 */

/**
 * Extends {@link ImageCodec} and serves as both an {@link ImageDecoder} and 
 * {@link ImageEncoder} for the PNG image storage format. Currently only supports RGB encodings.
 */
public class PngCodec extends ImageCodec { //implements ImageDecoder, ImageEncoder {

  private static final PngCodec staticObject = new PngCodec();
  
  /** black and white image mode. */
  private static final byte BW_MODE = 0;

  /** grey scale image mode. */
  private static final byte GREYSCALE_MODE = 1;

  /** full color image mode. */
  private static final byte COLOR_MODE = 2;
  
  private CRC32 crc;

  public static PngCodec getInstance() {
    return staticObject;
  }

  /**
   * Decodes the image header from an input stream that contains the PNG image. PNG images are
   * broken up into "chunks" (see PNG documentation), and the PNG header could be located anywhere
   * in the image
   * 
   * @param inputStream the {@link InputStream} that contains the PNG image
   * @return {@link HipiImageHeader} found in the input stream
   */
  public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException {

    DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
    dis.mark(Integer.MAX_VALUE);

    readSignature(dis);

    int width = -1;
    int height = -1;
    
    boolean trucking = true;
    while (trucking) {
      try {
	// Read the length.
	int length = dis.readInt();
	if (length <= 0)
	  throw new IOException("PNG file is too long to proceed. (Found length <= 0).");
	// Read the type.
	byte[] typeBytes = new byte[4];
	dis.readFully(typeBytes);
	String typeString = new String(typeBytes, "UTF8");
	if (typeString.equals("IHDR")) {
	  // Read the data.
	  byte[] data = new byte[length];
	  dis.readFully(data);
	  // Read the CRC.
	  long crc = dis.readInt() & 0x00000000ffffffffL; // Make it unsigned.
	  if (verifyCRC(typeBytes, data, crc) == false) {
	    throw new IOException("PNG file appears to be corrupted (unverifiable CRC).");
	  }
	  PNGChunk chunk = staticObject.new PNGChunk(typeBytes, data);
	  width = (int)chunk.getUnsignedInt(0);
	  height = (int)chunk.getUnsignedInt(4);
	  break;
	} else {
	  // Skip data + CRC signature.
	  dis.skipBytes(length + 4);
	}
      } catch (EOFException eofe) {
	trucking = false;
      }
    }

    if (width <= 0 || height <= 0) {
      throw new IOException("Failed to decode PNG image header. (Found invalid dimensions width <= 0 or height <= 0.)");
    }

    HashMap<String,String> exifData = null;
    if (includeExifData) {
      dis.reset();
      exifData = ExifDataReader.extractAndFlatten(dis);
    }

    return new HipiImageHeader(HipiImageFormat.PNG, HipiColorSpace.RGB,
			       width, height, 3, null, exifData);
  }

  protected static void readSignature(DataInputStream in) throws IOException {
    long signature = in.readLong();
    if (signature != 0x89504e470d0a1a0aL)
      throw new IOException("PNG signature not found!");
  }

  protected static PNGData readChunks(DataInputStream in) throws IOException {
    PNGData chunks = staticObject.new PNGData();

    boolean trucking = true;
    while (trucking) {
      try {
        // Read the length.
        int length = in.readInt();
        if (length <= 0)
          throw new IOException("Found invalid length in PNG segment (length <= 0).");
        // Read the type.
        byte[] typeBytes = new byte[4];
        in.readFully(typeBytes);
        // Read the data.
        byte[] data = new byte[length];
        in.readFully(data);
        // Read the CRC.
        long crc = in.readInt() & 0x00000000ffffffffL; // Make it
        // unsigned.
        if (verifyCRC(typeBytes, data, crc) == false)
          throw new IOException("That file appears to be corrupted.");

        PNGChunk chunk = staticObject.new PNGChunk(typeBytes, data);
        chunks.add(chunk);
      } catch (EOFException eofe) {
        trucking = false;
      }
    }
    return chunks;
  }

  protected static boolean verifyCRC(byte[] typeBytes, byte[] data, long crc) {
    CRC32 crc32 = new CRC32();
    crc32.update(typeBytes);
    crc32.update(data);
    long calculated = crc32.getValue();
    return (calculated == crc);
  }

  class PNGData {
    private int mNumberOfChunks;

    private PNGChunk[] mChunks;

    public PNGData() {
      mNumberOfChunks = 0;
      mChunks = new PNGChunk[10];
    }

    public void printAll() {
      System.out.println("number of chunks: " + mNumberOfChunks);
      for (int i = 0; i < mChunks.length; i++)
        System.out.println("(" + mChunks[i].getTypeString() + ", " + ")");
    }

    public void add(PNGChunk chunk) {
      mChunks[mNumberOfChunks++] = chunk;
      if (mNumberOfChunks >= mChunks.length) {
        PNGChunk[] largerArray = new PNGChunk[mChunks.length + 10];
        System.arraycopy(mChunks, 0, largerArray, 0, mChunks.length);
        mChunks = largerArray;
      }
    }

    public long getWidth() {
      return getChunk("IHDR").getUnsignedInt(0);
    }

    public long getHeight() {
      return getChunk("IHDR").getUnsignedInt(4);
    }

    public short getBitsPerPixel() {
      return getChunk("IHDR").getUnsignedByte(8);
    }

    public short getColorType() {
      return getChunk("IHDR").getUnsignedByte(9);
    }

    public short getCompression() {
      return getChunk("IHDR").getUnsignedByte(10);
    }

    public short getFilter() {
      return getChunk("IHDR").getUnsignedByte(11);
    }

    public short getInterlace() {
      return getChunk("IHDR").getUnsignedByte(12);
    }

    public byte[] getImageData() {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Write all the IDAT data into the array.
        for (int i = 0; i < mNumberOfChunks; i++) {
          PNGChunk chunk = mChunks[i];
          if (chunk.getTypeString().equals("IDAT")) {
            out.write(chunk.getData());
          }
        }
        out.flush();
        // Now deflate the data.
        InflaterInputStream in =
            new InflaterInputStream(new ByteArrayInputStream(out.toByteArray()));
        ByteArrayOutputStream inflatedOut = new ByteArrayOutputStream();
        int readLength;
        byte[] block = new byte[8192];
        while ((readLength = in.read(block)) != -1)
          inflatedOut.write(block, 0, readLength);
        inflatedOut.flush();
        byte[] imageData = inflatedOut.toByteArray();
        // Compute the real length.
        int width = (int) getWidth();
        int height = (int) getHeight();
        int bitsPerPixel = getBitsPerPixel();
        int length = width * height * bitsPerPixel / 8 * 3; // hard code the 3 for RGB for now

        byte[] prunedData = new byte[length];

        // We can only deal with non-interlaced images.
        if (getInterlace() == 0) {
          int index = 0;
          for (int i = 0; i < length; i++) {
            if (i % (width * bitsPerPixel / 8 * 3) == 0) { // again, hard code the 3 for RGB
              index++; // Skip the filter byte.
            }
            prunedData[i] = imageData[index++];
          }
        } else
          System.out.println("Couldn't undo interlacing.");

        return prunedData;
      } catch (IOException ioe) {
      }
      return null;
    }

    public PNGChunk getChunk(String type) {
      for (int i = 0; i < mNumberOfChunks; i++)
        if (mChunks[i].getTypeString().equals(type))
          return mChunks[i];
      return null;
    }
  }

  class PNGChunk {
    private byte[] mType;

    private byte[] mData;

    public PNGChunk(byte[] type, byte[] data) {
      mType = type;
      mData = data;
    }

    public String getTypeString() {
      try {
        return new String(mType, "UTF8");
      } catch (UnsupportedEncodingException uee) {
        return "";
      }
    }

    public String getDataString() {
      try {
        return new String(mData, "UTF8");
      } catch (UnsupportedEncodingException uee) {
        return "";
      }
    }

    public byte[] getData() {
      return mData;
    }

    public long getUnsignedInt(int offset) {
      long value = 0;
      for (int i = 0; i < 4; i++)
        value += (mData[offset + i] & 0xff) << ((3 - i) * 8);
      return value;
    }

    public short getUnsignedByte(int offset) {
      return (short) (mData[offset] & 0x00ff);
    }
  }

  /**
   * Encodes an image in the PNG format.
   * 
   * @param image the input {@link HipiImage} to be encoded
   * @param os the {@link OutputStream} that the encoded image will be written to
   */
  public void encodeImage(HipiImage image, OutputStream os) throws IllegalArgumentException, IOException {

    if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
      throw new IllegalArgumentException("PNG encoder supports only RasterImage input types.");
    }    

    if (image.getWidth() <= 0 || image.getHeight() <= 0) {
      throw new IllegalArgumentException("Invalid image dimensions.");
    }

    if (image.getColorSpace() != HipiColorSpace.RGB) {
      throw new IllegalArgumentException("PNG encoder supports only linear RGB color space.");
    }

    if (image.getNumBands() != 3) {
      throw new IllegalArgumentException("PNG encoder supports only three band images.");
    }

    crc = new CRC32();
    int width = image.getWidth();
    int height = image.getHeight();
    final byte id[] = {-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13};
    write(os, id);
    crc.reset();
    write(os, "IHDR".getBytes());
    write(os, width);
    write(os, height);
    byte head[] = null;

    int mode = COLOR_MODE;
    switch (mode) {
      case BW_MODE:
        head = new byte[] {1, 0, 0, 0, 0};
        break;
      case GREYSCALE_MODE:
        head = new byte[] {8, 0, 0, 0, 0};
        break;
      case COLOR_MODE:
        head = new byte[] {8, 2, 0, 0, 0};
        break;
    }
    write(os, head);
    write(os, (int) crc.getValue());
    ByteArrayOutputStream compressed = new ByteArrayOutputStream(65536);
    BufferedOutputStream bos =
        new BufferedOutputStream(new DeflaterOutputStream(compressed, new Deflater(9)));
    PixelArray pa = ((RasterImage)image).getPixelArray();
    switch (mode) {
      case COLOR_MODE:
        for (int y = 0; y < height; y++) {
          bos.write(0);
          for (int x = 0; x < width; x++) {
	    /*
            int r = Math.min(Math.max((int) (image.getPixel(x, y, 0) * 255), 0), 255);
            int g = Math.min(Math.max((int) (image.getPixel(x, y, 1) * 255), 0), 255);
            int b = Math.min(Math.max((int) (image.getPixel(x, y, 2) * 255), 0), 255);
	    */
            int r = pa.getElem((y*width+x)*3+0);
            int g = pa.getElem((y*width+x)*3+1);
            int b = pa.getElem((y*width+x)*3+2);
            bos.write((byte)r);
            bos.write((byte)g);
            bos.write((byte)b);
          }
        }
        break;
    }
    bos.close();
    write(os, compressed.size());
    crc.reset();
    write(os, "IDAT".getBytes());
    write(os, compressed.toByteArray());
    write(os, (int) crc.getValue());
    write(os, 0);
    crc.reset();
    write(os, "IEND".getBytes());
    write(os, (int) crc.getValue());
    os.close();
  }

  private void write(OutputStream os, int i) throws IOException {
    byte b[] =
        {(byte) ((i >> 24) & 0xff), (byte) ((i >> 16) & 0xff), (byte) ((i >> 8) & 0xff),
            (byte) (i & 0xff)};
    write(os, b);
  }

  private void write(OutputStream os, byte b[]) throws IOException {
    os.write(b);
    crc.update(b);
  }

}
