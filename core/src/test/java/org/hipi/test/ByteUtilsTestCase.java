package org.hipi.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.hipi.util.ByteUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteUtilsTestCase {

  private static final short[] shortArray = new short[] {6, 7, 8, 9, 10};
  private static byte[] convertedShortArray;

  private static final int[] intArray = new int[] {11, 12, 13, 14, 15};
  private static byte[] convertedIntArray;


  private static final float[] floatArray = new float[] {16.0f, 17.0f, 18.0f, 19.0f, 20.0f};
  private static byte[] convertedFloatArray;

  private static final double[] doubleArray = new double[] {21.0, 22.0, 23.0, 24.0, 25.0};
  private static byte[] convertedDoubleArray;

  private static final double delta = 0.01;



  private byte[] buildConvertedShortArray() {
    ByteBuffer bb = ByteBuffer.allocate(shortArray.length * 2);
    for (int i = 0; i < shortArray.length; i++) {
      bb.putShort(shortArray[i]);
    }
    return bb.array();
  }

  @Test
  public void testShortArrayToByteArray() {
    convertedShortArray = buildConvertedShortArray();
    byte[] newBytes = ByteUtils.shortArrayToByteArray(shortArray);
    assertEquals(0, newBytes.length % 2);
    assertArrayEquals(convertedShortArray, newBytes);
  }

  @Test
  public void testByteArrayToShortArray() {
    short[] newShorts = ByteUtils.byteArrayToShortArray(buildConvertedShortArray());
    assertEquals(shortArray.length, newShorts.length);
    assertArrayEquals(shortArray, newShorts);
  }

  private byte[] buildConvertedIntArray() {
    ByteBuffer bb = ByteBuffer.allocate(intArray.length * 4);
    for (int i = 0; i < intArray.length; i++) {
      bb.putInt(intArray[i]);
    }
    return bb.array();
  }

  @Test
  public void testIntArrayToByteArray() {
    convertedIntArray = buildConvertedIntArray();
    byte[] newBytes = ByteUtils.intArrayToByteArray(intArray);
    assertEquals(0, newBytes.length % 4);
    assertArrayEquals(convertedIntArray, newBytes);
  }

  @Test
  public void testByteArrayToIntArray() {
    int[] newInts = ByteUtils.byteArrayToIntArray(buildConvertedIntArray());
    assertEquals(intArray.length, newInts.length);
    assertArrayEquals(intArray, newInts);
  }

  private byte[] buildConvertedFloatArray() {
    ByteBuffer bb = ByteBuffer.allocate(floatArray.length * 4);
    for (int i = 0; i < floatArray.length; i++) {
      bb.putFloat(floatArray[i]);
    }
    return bb.array();
  }

  @Test
  public void testFloatArrayToByteArray() {
    convertedFloatArray = buildConvertedFloatArray();
    byte[] newBytes = ByteUtils.floatArrayToByteArray(floatArray);
    assertEquals(0, newBytes.length % 4);
    assertArrayEquals(convertedFloatArray, newBytes);
  }

  @Test
  public void testByteArrayToFloatArray() {
    float[] newFloats = ByteUtils.byteArrayToFloatArray(buildConvertedFloatArray());
    assertEquals(floatArray.length, newFloats.length);
    assertArrayEquals(floatArray, newFloats, (float) delta);
  }

  private byte[] buildConvertedDoubleArray() {
    ByteBuffer bb = ByteBuffer.allocate(doubleArray.length * 8);
    for (int i = 0; i < doubleArray.length; i++) {
      bb.putDouble(doubleArray[i]);
    }
    return bb.array();
  }

  @Test
  public void testDoubleArrayToByteArray() {
    convertedDoubleArray = buildConvertedDoubleArray();
    byte[] newBytes = ByteUtils.doubleArrayToByteArray(doubleArray);
    assertEquals(0, newBytes.length % 8);
    assertArrayEquals(convertedDoubleArray, newBytes);
  }

  @Test
  public void testByteArrayToDoubleArray() {
    double[] newDoubles = ByteUtils.byteArrayToDoubleArray(buildConvertedDoubleArray());
    assertEquals(doubleArray.length, newDoubles.length);
    assertArrayEquals(doubleArray, newDoubles, delta);
  }



}
