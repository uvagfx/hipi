package hipi.util;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ByteUtils {

	/**
	 * Convert from an array of floats to an array of bytes
	 * @param floatArray
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
	 */
	public static final int ByteArrayToInt(byte[] byteArray) {
		return ByteArrayToInt(byteArray, 0);
	}
	
	/**
	 * Convert from a byte array at an offset to one int
	 * @param byteArray
	 * @param offset the offset in the byteArray that is the first byte of the int
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
	 */
	public static final byte[] IntToByteArray(int i) {
		return new byte[] { (byte)(i>>24), (byte)(i>>16), (byte)(i>>8), (byte)i };
	}
	
	/**
	 * 
	 * @param vals
	 * @return A hex string of the input according to SHA-1 standards
	 */
	public static String asHex(byte[] vals) {
		try {
			MessageDigest sha1;
			sha1 = MessageDigest.getInstance("SHA-1");
			byte[] bytes = sha1.digest(vals);
			StringBuilder hex = new StringBuilder(bytes.length * 2);
			for (int i = 0; i < bytes.length; i++)
				hex.append(Integer.toHexString(0xFF & bytes[i]));
			return hex.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
	}
}
