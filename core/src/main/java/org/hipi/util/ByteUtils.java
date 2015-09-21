package org.hipi.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.nio.DoubleBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Various static helper methods that come in handy when serializing and deserializing arrays of
 * different Java types and performing certain operations with byte arrays like conversion to
 * string and computing hashes.
 */
public class ByteUtils {

  /**
   * Reads the contents of an stream until exhausted and converts contents to an array of bytes.
   *
   * @param stream
   */
  public static byte[] inputStreamToByteArray(InputStream stream) throws IOException {
    if (stream == null) {
      return new byte[] {};
    }
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    boolean error = false;
    try {
      int numRead = 0;
      while ((numRead = stream.read(buffer)) > -1) {
        output.write(buffer, 0, numRead);
      }
    } catch (IOException ioe) {
      error = true; // this error should be thrown, even if there is an error closing stream
      throw ioe;
    } catch (RuntimeException re) {
      error = true; // this error should be thrown, even if there is an error closing stream
      throw re;
    } finally {
      try {
        stream.close();
      } catch (IOException ioe) {
        if (!error) {
          throw ioe;
        }
      }
    }
    output.flush();
    return output.toByteArray();
  }


  /**
   * Convert from a byte array to one int
   * 
   * @param byteArray
   */
  public static final int byteArrayToInt(byte[] byteArray) {
    return byteArrayToInt(byteArray, 0);
  }

  /**
   * Convert from a byte array at an offset to one int
   * 
   * @param byteArray
   * @param offset the offset in the byteArray that is the first byte of the int
   * 
   * TODO: Test that this will work for leading-zero bytes
   */
  public static final int byteArrayToInt(byte[] byteArray, int offset) {
    return byteArray[0 + offset] << 24 | (byteArray[1 + offset] & 0xff) << 16 | 
        (byteArray[2 + offset] & 0xff) << 8 | (byteArray[3 + offset] & 0xff);
  }

  /**
   * Convert from one int to a byte array
   * 
   * @param i the integer
   */
  public static final byte[] intToByteArray(int i) {
    return new byte[] {(byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i};
  }

  /**
   * Computes SHA-1 hash of byte array.
   * 
   * @param vals input byte array
   * @return SHA-1 hash of the input byte array
   */
  public static String asHex(byte[] vals) {
    if (vals == null) {
      return null;
    }
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

  public static String makeString(byte[] vals, int count) {
    if (vals == null || Math.min(vals.length, count) <= 0) {
      return "";
    }
    int n = Math.min(vals.length, count);
    String result = "";
    for (int i=0; i<n; i++) {
      result += String.format("0x%02X", vals[i]);
      if (i<(n-1)) {
	result += " ";
      }
    }
    return result;
  }

  /**
   * Convert from an array of bytes to an array of shorts
   * @param byteArray
   */
  public static short[] byteArrayToShortArray(byte[] byteArray) {
    
    if(byteArray.length % 2 != 0) {
      throw new IllegalArgumentException("Length of byteArray is not evenly divisible by 2, "
          + "which is the number of bytes in one short.");
    }
    short shortArray[] = new short[byteArray.length / 2];
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    ShortBuffer shortBuff = byteBuf.asShortBuffer();
    shortBuff.get(shortArray);
    return shortArray;
  }
  
  /**
   * Convert from an array of shorts to an array of bytes
   * 
   * @param shortArray
   */
  public static byte[] shortArrayToByteArray(short shortArray[]) {
    byte byteArray[] = new byte[shortArray.length*2]; // 2 bytes per short
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    ShortBuffer shortBuf = byteBuf.asShortBuffer();
    shortBuf.put(shortArray);
    return byteArray;
  }

  
  /** 
   * Convert from an array of bytes to an array of ints
   * @param byteArray
   */
  public static int[] byteArrayToIntArray(byte[] byteArray) {
    if(byteArray.length % 4 != 0) {
      throw new IllegalArgumentException("Length of byteArray is not evenly divisible by 4, "
          + "which is the number of bytes in one int.");
    }
    int intArray[] = new int[byteArray.length / 4];
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    IntBuffer intBuff = byteBuf.asIntBuffer();
    intBuff.get(intArray);
    return intArray;
  }

  /**
   * Convert from an array of ints to an array of bytes
   * @param intArray
   */
  public static byte[] intArrayToByteArray(int[] intArray) {
    byte[] byteArray = new byte[intArray.length*4]; // 4 bytes per int
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    IntBuffer intBuff = byteBuf.asIntBuffer();
    intBuff.put(intArray);
    return byteArray;
  }

  /**
   * Convert from an array of bytes to an array of floats
   * 
   * @param byteArray
   */
  public static float[] byteArrayToFloatArray(byte byteArray[]) throws IllegalArgumentException {
    if (byteArray.length % 4 != 0) {
      throw new IllegalArgumentException("Length of byteArray is not evenly divisible by 4, "
          + "which is the number of bytes in one float.");
    }
    float floatArray[] = new float[byteArray.length / 4];
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    FloatBuffer floatBuf = byteBuf.asFloatBuffer();
    floatBuf.get(floatArray);
    return floatArray;
  }
  
  /**
   * Convert from an array of floats to an array of bytes
   * 
   * @param floatArray
   */
  public static byte[] floatArrayToByteArray(float floatArray[]) {
    byte byteArray[] = new byte[floatArray.length*4]; // 4 bytes per float
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    FloatBuffer floatBuf = byteBuf.asFloatBuffer();
    floatBuf.put(floatArray);
    return byteArray;
  }
  
  /**
   * Convert from an array of bytes to an array of doubles
   * @param byteArray
   */
  public static double[] byteArrayToDoubleArray(byte[] byteArray) {
    if (byteArray.length % 8 != 0) {
      throw new IllegalArgumentException("Length of byteArray is not evenly divisible by 8, "
          + "which is the number of bytes in one double.");
    }
    double doubleArray[] = new double[byteArray.length / 8];
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    DoubleBuffer doubleBuff = byteBuf.asDoubleBuffer();
    doubleBuff.get(doubleArray);
    return doubleArray;
  }

  /**
   * Convert from an array of doubles to an array of bytes
   * @param doubleArray
   */
  public static byte[] doubleArrayToByteArray(double[] doubleArray) {
    byte[] byteArray = new byte[doubleArray.length*8]; // 8 bytes per double
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArray);
    DoubleBuffer doubleBuff = byteBuf.asDoubleBuffer();
    doubleBuff.put(doubleArray);
    return byteArray;
  }

}
