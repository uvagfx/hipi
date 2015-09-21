package org.hipi.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.hipi.image.PixelArray;
import org.hipi.image.PixelArrayFloat;
import org.hipi.image.PixelArrayByte;

import org.junit.Test;
import org.junit.Ignore;

public class PixelArrayTestCase {

  @Ignore
  @Test
  public void testPrintSRGBLUT() {
    // gamma compressed 8-bit sRGB => linear floating point RGB
    System.out.print("private static final float[] gammaExpand = {");
    for (int i=0; i<256; i++) {
      double nonlinear = ((double)i)/255.0;
      double linear = (float)( ( nonlinear <= 0.04045 )
			       ? ( nonlinear / 12.92 )
			       : ( Math.pow( (nonlinear+0.055)/1.055, 2.4 ) ) );
      System.out.print(linear);
      System.out.print("f");
      if (i<(256-1)) {
	System.out.print(",");
      }
    }
    System.out.print("};");

    // linear floating-point RGB => gamma compressed 8-bit sRGB
    int n = 2048;
    System.out.print("private static final byte[] gammaCompress = {");
    for (int i=0; i<n; i++) {
      double linear = ((double)i+0.5)/(double)(n-1);
      double nonlinear = ( ( linear <= 0.0031308 )
			   ? ( 12.92 * linear )
			   : ( 1.055 * Math.pow( linear, 1.0/2.4 ) - 0.055 ) );
      int srgb = (int)(Math.max(0,Math.min(255,(int)(nonlinear*255.0))));
      System.out.print(srgb);
      if (i<(n-1)) {
	System.out.print(",");
      }
    }
    System.out.print("};");

  }

  @Test
  public void testAllocations() {
    {
      PixelArrayByte pa = new PixelArrayByte(1);
      assertEquals(PixelArray.TYPE_BYTE, pa.getDataType());
      assertEquals(1,PixelArray.getDataTypeSize(pa.getDataType()));

      assertEquals(1,pa.getSize());
      assertNotNull(pa.getData());
      assertEquals(0,pa.getElem(0));
      
      pa.setSize(10);
      assertEquals(10,pa.getSize());
      assertNotNull(pa.getData());
      
      pa.setSize(0);
      assertEquals(0,pa.getSize());
      assertNull(pa.getData());
    }
    {
      PixelArrayFloat pa = new PixelArrayFloat(1);
      assertEquals(PixelArray.TYPE_FLOAT, pa.getDataType());
      assertEquals(4,PixelArray.getDataTypeSize(pa.getDataType()));

      assertEquals(1,pa.getSize());
      assertNotNull(pa.getData());
      
      pa.setSize(10);
      assertEquals(10,pa.getSize());
      assertNotNull(pa.getData());
      
      pa.setSize(0);
      assertEquals(0,pa.getSize());
      assertNull(pa.getData());
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidSizePixelArrayByte() {
    PixelArrayByte pa = new PixelArrayByte(100);
    assertEquals(100, pa.getSize());
    pa.setSize(-1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidSizePixelArrayFloat() {
    PixelArrayFloat pa = new PixelArrayFloat(100);
    assertEquals(100, pa.getSize());
    pa.setSize(-1);
  }

  @Test(expected=ArrayIndexOutOfBoundsException.class)
  public void testOutOfBounds() {
    PixelArrayFloat pa = new PixelArrayFloat(100);
    pa.setElem(100,127);
  }

  @Test
  public void testByteConversions() {
    PixelArrayByte pa = new PixelArrayByte(10);

    // set
    pa.setElem(6, 200);
    assertEquals("200 => 200.0f/255.0f", 200.0f/255.0f, pa.getElemFloat(6), 1e-5);
    assertEquals("200 => 200.0/255.0", 200.0/255.0, pa.getElemDouble(6), 1e-5);

    // setFloat
    pa.setElemFloat(2, 0.5f);
    assertEquals(127, pa.getElem(2));
    assertEquals(0.5f, pa.getElemFloat(2), 1e-1);
    assertEquals(0.5, pa.getElemDouble(2), 1e-1);
    pa.setElemFloat(1, 0.2f);
    assertEquals(51, pa.getElem(1));
    assertEquals(0.2f, pa.getElemFloat(1), 1e-5);
    assertEquals(0.2, pa.getElemDouble(1), 1e-5);

    // setDouble
    pa.setElemDouble(4, 0.5f);
    assertEquals(127, pa.getElem(4));
    assertEquals(0.5f, pa.getElemFloat(4), 1e-1);
    assertEquals(0.5, pa.getElemDouble(4), 1e-1);
    pa.setElemDouble(3, 0.2f);
    assertEquals(51, pa.getElem(3));
    assertEquals(0.2f, pa.getElemFloat(3), 1e-5);
    assertEquals(0.2, pa.getElemDouble(3), 1e-5);
  }

  @Test
  public void testFloatConversions() {
    PixelArrayFloat pa = new PixelArrayFloat(10);

    // set
    pa.setElem(6, 200);
    assertEquals("200 => 200.0f/255.0f", 200.0f/255.0f, pa.getElemFloat(6), 1e-5);
    assertEquals("200 => 200.0/255.0", 200.0/255.0, pa.getElemDouble(6), 1e-5);

    // setFloat
    pa.setElemFloat(2, 0.5f);
    assertEquals(127, pa.getElem(2));
    assertEquals(0.5f, pa.getElemFloat(2), 1e-1);
    assertEquals(0.5, pa.getElemDouble(2), 1e-1);
    pa.setElemFloat(1, 0.2f);
    assertEquals(51, pa.getElem(1));
    assertEquals(0.2f, pa.getElemFloat(1), 1e-5);
    assertEquals(0.2, pa.getElemDouble(1), 1e-5);

    // setDouble
    pa.setElemDouble(4, 0.5);
    assertEquals(127, pa.getElem(4));
    assertEquals(0.5f, pa.getElemFloat(4), 1e-1);
    assertEquals(0.5, pa.getElemDouble(4), 1e-1);
    pa.setElemDouble(3, 0.2);
    assertEquals(51, pa.getElem(3));
    assertEquals(0.2f, pa.getElemFloat(3), 1e-5);
    assertEquals(0.2, pa.getElemDouble(3), 1e-5);
  }

  @Test
  public void testClamp() {
    {
      PixelArrayByte pa = new PixelArrayByte(10);

      pa.setElem(9, 200);
      assertEquals("200 => 200", 200, pa.getElem(9));
      pa.setElem(9, 255);
      assertEquals("255 => 255", 255, pa.getElem(9));
      pa.setElem(9, -1);
      assertEquals("-1 => 0", 0, pa.getElem(9));
      pa.setElem(9, -10);
      assertEquals("-10 => 0", 0, pa.getElem(9));
      pa.setElem(9, 0);
      assertEquals("0 => 0", 0, pa.getElem(9));
      pa.setElem(9, 100);
      assertEquals("100 => 100", 100, pa.getElem(9));
      pa.setElem(9, 256);
      assertEquals("256 => 255", 255, pa.getElem(9));
      pa.setElem(9, 348394);
      assertEquals("348394 => 255", 255, pa.getElem(9));
      
      pa.setElemFloat(0, -1.0f);
      assertEquals("-1.0f => 0", 0, pa.getElem(0));
      pa.setElemFloat(0, 0.f);
      assertEquals("0.f => 0", 0, pa.getElem(0));      
      pa.setElemDouble(0, 1.0);
      assertEquals("1.0 => 255", 255, pa.getElem(0));
      pa.setElemDouble(0, 1.2);
      assertEquals("1.2 => 255", 255, pa.getElem(0));
      pa.setElemDouble(0, 298.0);
      assertEquals("298.0 => 255", 255, pa.getElem(0));
    }
  }

}
