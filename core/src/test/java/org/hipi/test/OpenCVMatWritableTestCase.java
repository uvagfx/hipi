package org.hipi.test;

import static org.junit.Assert.*;

import org.hipi.opencv.OpenCVMatWritable;

import org.junit.Assert;
import org.junit.Test;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Size;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;

public class OpenCVMatWritableTestCase {
  
  private final double delta = 0.01;
  
  @Test
  public void testDefaultConstructorMatDimensions() {
    OpenCVMatWritable openCvMatWritable = new OpenCVMatWritable();
    Mat defaultMat = openCvMatWritable.getMat();
    int dims = defaultMat.dims();
    assertEquals("Dims are invalid (not 1 or 2): " + dims, true, (dims == 1 || dims == 2));
  }
  
  @Test
  public void testDefaultConstructorMatParameters() {
    OpenCVMatWritable openCvMatWritable = new OpenCVMatWritable();
    Mat defaultMat = openCvMatWritable.getMat();
    assertEquals(0, defaultMat.rows());
    assertEquals(0, defaultMat.cols());
    assertEquals(opencv_core.CV_8UC1, defaultMat.type());
  }
  
  @Test
  public void testMatConstructor() {
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_32FC1);
    OpenCVMatWritable openCvMatWritable = new OpenCVMatWritable(inputMat);
    Mat outputMat = openCvMatWritable.getMat();
    assertEquals(2, outputMat.dims());
    assertEquals(3.0, outputMat.rows(), delta);
    assertEquals(5.0, outputMat.cols(), delta);
    assertEquals(opencv_core.CV_32FC1, outputMat.type());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testSetMatWithNullInput() {
    Mat inputMat = null;
    OpenCVMatWritable openCvMatWritable = new OpenCVMatWritable();
    openCvMatWritable.setMat(inputMat);
  }
  
  @Test
  public void serializeAndRecreateSignedByteMat() {
    byte[] testData = new byte[] {-1, 2, -3, 4, -5, 6, -7, 8, -9, 10, -11, 12, -13, 14, -15};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_8SC1);
    ((ByteBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_8SC1, recreatedMat.type());
    
    byte[] recreatedTestData =  new byte[testData.length];
    ((ByteBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData);
  }
  
  @Test
  public void serializeAndRecreateSignedShortMat() {
    short[] testData = new short[] {-1, 2, -3, 4, -5, 6, -7, 8, -9, 10, -11, 12, -13, 14, -15};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_16SC1);
    ((ShortBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_16SC1, recreatedMat.type());
    
    short[] recreatedTestData = new short[testData.length];
    ((ShortBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData);
  }
  
  @Test
  public void serializeAndRecreateUnsignedShortMat() {
    short[] testData = new short[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_16UC1);
    ((ShortBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_16UC1, recreatedMat.type());
    
    short[] recreatedTestData = new short[testData.length];
    ((ShortBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData);
  }
  
  @Test
  public void serializeAndRecreateSignedIntMat() {
    int[] testData = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_32SC1);
    ((IntBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_32SC1, recreatedMat.type());
    
    int[] recreatedTestData = new int[testData.length];
    ((IntBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData);
  }
  
  @Test
  public void serializeAndRecreateFloatMat() {
    
    float [] testData = new float [] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 14.0f, 15.0f};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_32FC1);
    ((FloatBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_32FC1, recreatedMat.type());
    
    float[] recreatedTestData = new float[testData.length];
    ((FloatBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData, (float)delta);
  }
  
  @Test
  public void serializeAndRecreateDoubleMat() {
    
    double [] testData = new double [] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0};
    
    Mat inputMat = new Mat(new Size(5, 3), opencv_core.CV_64FC1);
    ((DoubleBuffer) inputMat.createBuffer()).put(testData);
    
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable(inputMat);
    
    ByteArrayOutputStream baos = serializeOpenCVMatWritable(openCVMatWritable);
    OpenCVMatWritable newOpenCVMatWritable = deserializeOpenCVMatWritable(baos);
    
    Mat recreatedMat = newOpenCVMatWritable.getMat();
    
    assertEquals(3, recreatedMat.rows());
    assertEquals(5, recreatedMat.cols());
    assertEquals(opencv_core.CV_64FC1, recreatedMat.type());
    
    double[] recreatedTestData = new double[testData.length];
    ((DoubleBuffer) recreatedMat.createBuffer()).get(recreatedTestData);
    Assert.assertArrayEquals(testData, recreatedTestData, delta);
  }
  
  private ByteArrayOutputStream serializeOpenCVMatWritable(OpenCVMatWritable openCVMatWritable) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    DataOutput out = new DataOutputStream(baos);
    try {
      openCVMatWritable.write(out);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      fail();
    }
    return baos;
  }
  
  private OpenCVMatWritable deserializeOpenCVMatWritable(ByteArrayOutputStream baos) {
    DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    
    OpenCVMatWritable newOpenCVMatWritable = new OpenCVMatWritable();
    try {
      newOpenCVMatWritable.readFields(in);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      fail();
    }
    return newOpenCVMatWritable;
  }
  
  
  
  

}
