package hipi.util;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

public class ByteUtils {

	/**
	 * Convert from an array of floats to an array of bytes
	 * @param floatArray
	 * @return
	 */
	public static byte[] FloatArraytoByteArray(float floatArray[]) { 
		byte byteArray[] = new byte[floatArray.length*4];
		
		ByteBuffer byteBuf = ByteBuffer.wrap(byteArray); 
		
		FloatBuffer floatBuf = byteBuf.asFloatBuffer();
		
		floatBuf.put (floatArray);
		
		return byteArray; 
	} 

	/**
	 * Convert from an array of bytes to an array of floats
	 * @param byteArray
	 * @return
	 */
	public static float[] ByteArraytoFloatArray(byte byteArray[]) { 
		float floatArray[] = new float[byteArray.length/4]; 

		ByteBuffer byteBuf = ByteBuffer.wrap(byteArray); 

		FloatBuffer floatBuf = byteBuf.asFloatBuffer(); 

		floatBuf.get (floatArray); 

		return floatArray; 
	} 
	
	/**
	 * Convert from a byte array to one int
	 * @param byteArray
	 * @return
	 */
	public static final int ByteArrayToInt(byte[] byteArray) {
		return ByteArrayToInt(byteArray, 0);
	}
	
	/**
	 * Convert from a byte array at an offset to one int
	 * @param byteArray
	 * @param offset the offset in the byteArray that is the first byte of the int
	 * @return
	 * 
	 * TODO: Test that this will work for leading-zero bytes
	 */
	public static final int ByteArrayToInt(byte[] byteArray, int offset) {
		return byteArray[0+offset]<<24 | 
				(byteArray[1+offset]&0xff)<<16 | 
				(byteArray[2+offset]&0xff)<<8 | 
				(byteArray[3+offset]&0xff);
	}
	
	/**
	 * Convert from one int to a byte array
	 * @param i the integer
	 * @return
	 */
	public static final byte[] IntToByteArray(int i) {
		return new byte[] { (byte)(i>>24), (byte)(i>>16), (byte)(i>>8), (byte)i };
	}
}
