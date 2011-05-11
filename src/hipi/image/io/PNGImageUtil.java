package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 1997-2007 Sun Microsystems, Inc. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common
 * Development and Distribution License("CDDL") (collectively, the
 * "License"). You may not use this file except in compliance with the
 * License. You can obtain a copy of the License at
 * http://www.netbeans.org/cddl-gplv2.html
 * or nbbuild/licenses/CDDL-GPL-2-CP. See the License for the
 * specific language governing permissions and limitations under the
 * License.  When distributing the software, include this License Header
 * Notice in each file and include the License file at
 * nbbuild/licenses/CDDL-GPL-2-CP.  Sun designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Sun in the GPL Version 2 section of the License file that
 * accompanied this code. If applicable, add the following below the
 * License Header, with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * Contributor(s): Alexandre Iline.
 *
 * The Original Software is the Jemmy library.
 * The Initial Developer of the Original Software is Alexandre Iline.
 * All Rights Reserved.
 *
 * If you wish your version of this file to be governed by only the CDDL
 * or only the GPL Version 2, indicate your decision by adding
 * "[Contributor] elects to include this software in this distribution
 * under the [CDDL or GPL Version 2] license." If you do not indicate a
 * single choice of license, a recipient has the option to distribute
 * your version of this file under either the CDDL, the GPL Version 2 or
 * to extend the choice of license to its licensees as provided above.
 * However, if you add GPL Version 2 code and therefore, elected the GPL
 * Version 2 license, then the option applies only if the new code is
 * made subject to such option by the copyright holder.
 *
 *
 *
 * Heavy modifications were made to the original library by Chris Sweeney
 *
 */

/**
 * Currently, images can only be encoded and decoded with RGB encoding. That is, black and white, and grayscale
 * encoded images cannot be used.
 */
public class PNGImageUtil implements ImageDecoder, ImageEncoder{


	private static final PNGImageUtil static_object = new PNGImageUtil();
	/** black and white image mode. */    
	private static final byte BW_MODE = 0;
	/** grey scale image mode. */    
	private static final byte GREYSCALE_MODE = 1;
	/** full color image mode. */    
	private static final byte COLOR_MODE = 2;
	private CRC32 crc;
	public static PNGImageUtil getInstance() {
		return static_object;
	}
	
	/**
	 * Decodes the image header from an input stream that contains the PNG image. PNG images are broken up into "chunks"
	 * (see PNG documentation), and the PNG header could be located anywhere in the image
	 * 
	 * @param is The {@link InputStream} that contains the PNG image
	 * @return The {@link ImageHeader} found in the input stream
	 */
	public ImageHeader decodeImageHeader(InputStream is) throws IOException {
		ImageHeader header = new ImageHeader();
		DataInputStream in = new DataInputStream(is);
		readSignature(in);

		boolean trucking = true;
		while (trucking) {
			try {
				// Read the length.
				int length = in.readInt();
				if (length < 0)
					throw new IOException("Sorry, that file is too long.");
				// Read the type.
				byte[] typeBytes = new byte[4];
				in.readFully(typeBytes);
				String typeString = new String(typeBytes, "UTF8");
				if(typeString.equals("IHDR")) {
					// Read the data.
					byte[] data = new byte[length];
					in.readFully(data);
					// Read the CRC.
					long crc = in.readInt() & 0x00000000ffffffffL; // Make it
					// unsigned.
					if (verifyCRC(typeBytes, data, crc) == false)
						throw new IOException("That file appears to be corrupted.");

					PNGChunk chunk = static_object.new PNGChunk(typeBytes, data);
					header.width      = (int) chunk.getUnsignedInt(0);
					header.height     = (int) chunk.getUnsignedInt(4);
					header.bitDepth  = chunk.getUnsignedByte(8);
					break;
				}
				else {
					// skip the data associated, plus the crc signature
					in.skipBytes(length+4);
				}
			} catch (EOFException eofe) {
				trucking = false;
			}
		}
		return header;
	}
	
	/**
	 * Decodes a PNG image from an input stream. This method only creates a {@link FloatImage} and will not
	 * create a {@link ImageHeader}. {@link #decodeImageHeader(InputStream)} must be called in order to acquire an {@link ImageHeader}
	 * 
	 * @param is The {@link InputStream} that contains the PNG image
	 * @return The {@link FloatImage} from the input stream
	 */
	public FloatImage decodeImage(InputStream is) throws IOException {		
		DataInputStream dataIn = new DataInputStream(is);
		readSignature(dataIn);
		PNGData chunks = readChunks(dataIn);

		long widthLong = chunks.getWidth();
		long heightLong = chunks.getHeight();
		if (widthLong > Integer.MAX_VALUE || heightLong > Integer.MAX_VALUE)
			throw new IOException("That image is too wide or tall.");
		int width = (int) widthLong;
		int height = (int) heightLong;
		float[] pels = new float[width * height * 3];
		byte[] image_bytes = chunks.getImageData();

		for(int i = 0; i < image_bytes.length; i++)
			pels[i] = (float) ((image_bytes[i]&0xff)/255.0);
		FloatImage image = new FloatImage(width, height, 3, pels); //hard code 3

		return image;
	}

	protected static void readSignature(DataInputStream in) throws IOException {
		long signature = in.readLong();
		if (signature != 0x89504e470d0a1a0aL)
			throw new IOException("PNG signature not found!");
	}

	protected static PNGData readChunks(DataInputStream in) throws IOException {
		PNGData chunks = static_object.new PNGData();

		boolean trucking = true;
		while (trucking) {
			try {
				// Read the length.
				int length = in.readInt();
				if (length < 0)
					throw new IOException("Sorry, that file is too long.");
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

				PNGChunk chunk = static_object.new PNGChunk(typeBytes, data);
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
		public void printAll(){
			System.out.println("number of chunks: " + mNumberOfChunks);
			for(int i = 0; i < mChunks.length; i++)
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

		public long getHeight() {    return getChunk("IHDR").getUnsignedInt(4);
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
				InflaterInputStream in = new InflaterInputStream(
						new ByteArrayInputStream(out.toByteArray()));
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
				int length = width * height * bitsPerPixel / 8 * 3; //hard code the 3 for RGB for now

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

	public ImageHeader createSimpleHeader(FloatImage image) {
		return new ImageHeader(ImageType.PNG_IMAGE);
	}
	
	/**
	 * Encodes an image into a PNG image
	 * 
	 * @param image The {@link FloatImage} that contains the PNG image
	 * @param header The {@link ImageHeader} for the image
	 * @param os The {@link OutputStream} that the encoded image will write to
	 */
	public void encodeImage(FloatImage image, ImageHeader header,
			OutputStream os) throws IOException {
		crc = new CRC32();
		int width = image.getWidth();
		int height = image.getHeight();
		final byte id[] = {-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13};
		write(os, id);
		crc.reset();
		write(os, "IHDR".getBytes());
		write(os, width);
		write(os, height);
		byte head[]=null;

		int mode = COLOR_MODE;
		switch (mode) {
		case BW_MODE: head=new byte[]{1, 0, 0, 0, 0}; break;
		case GREYSCALE_MODE: head=new byte[]{8, 0, 0, 0, 0}; break;
		case COLOR_MODE: head=new byte[]{8, 2, 0, 0, 0}; break;
		}                 
		write(os, head);
		write(os, (int)crc.getValue());
		ByteArrayOutputStream compressed = new ByteArrayOutputStream(65536);
		BufferedOutputStream bos = new BufferedOutputStream( new DeflaterOutputStream(compressed, new Deflater(9)));
		switch (mode) {
		case COLOR_MODE:
			for (int y=0;y<height;y++) {
				bos.write(0);
				for (int x=0;x<width;x++) {
					int r = Math.min(Math.max((int)(image.getPixel(x, y, 0)*255), 0), 255);
					int g = Math.min(Math.max((int)(image.getPixel(x, y, 1)*255), 0), 255);
					int b = Math.min(Math.max((int)(image.getPixel(x, y, 2)*255), 0), 255);
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
		byte b[]={(byte)((i>>24)&0xff),(byte)((i>>16)&0xff),(byte)((i>>8)&0xff),(byte)(i&0xff)};
		write(os, b);
	}

	private void write(OutputStream os, byte b[]) throws IOException {
		os.write(b);
		crc.update(b);
	}

}
