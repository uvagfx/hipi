package org.hipi.image;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.image.RasterImage;
import org.hipi.image.PixelArrayFloat;
import org.hipi.util.ByteUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jocl.CL;
import org.jocl.Pointer;
import org.jocl.Sizeof;
import org.jocl.cl_command_queue;
import org.jocl.cl_context;
import org.jocl.cl_context_properties;
import org.jocl.cl_device_id;
import org.jocl.cl_kernel;
import org.jocl.cl_mem;
import org.jocl.cl_platform_id;
import org.jocl.cl_program;

/**
 * A raster image represented as an array of Java floats. A FloatImage consists
 * of a flat array of pixel values represented as a {@link PixelArrayFloat} 
 * object along with a {@link HipiImageHeader} object.
 *
 * Note that individual pixel values in a FloatImage are understood to be in a linear
 * color space. We suggest using the {@link PixelArray#setElemNonLinSRGB} method to
 * set pixel values from 8-bit pixel values that are read from an image as these are
 * usually represented in a non-linear gamma-compressed color space. The {@link PixelArrayFloat}
 * class has routines for performing the conversion between linear and non-linear
 * color spaces (e.g., sRGB and linear RGB).
 *
 * The {@link org.hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) FloatImage objects in various
 * compressed and uncompressed image formats such as JPEG and PNG.
 */
public class FloatImage extends RasterImage {

  public FloatImage() {
    super((PixelArray)(new PixelArrayFloat()));
  }

  public FloatImage(int width, int height, int bands) throws IllegalArgumentException {
    super((PixelArray)(new PixelArrayFloat()));
    HipiImageHeader header = new HipiImageHeader(HipiImageFormat.UNDEFINED, HipiColorSpace.UNDEFINED, width, height, bands, null, null);
    setHeader(header);
  }

  public FloatImage(int width, int height, int bands, HipiImageFormat imgFormat, HipiColorSpace colorspace) throws IllegalArgumentException {
    super((PixelArray)(new PixelArrayFloat()));
    HipiImageHeader header = new HipiImageHeader(imgFormat, colorspace,
                         width, height, bands, null, null);
    setHeader(header);
  }

  public FloatImage(int width, int height, int bands, float[] data) 
  throws IllegalArgumentException {
    super((PixelArray)(new PixelArrayFloat()));
    HipiImageHeader header = new HipiImageHeader(HipiImageFormat.UNDEFINED, HipiColorSpace.UNDEFINED,
             width, height, bands, null, null);
    setHeader(header);
    if (data == null || data.length != width*height*bands) {
      throw new IllegalArgumentException("Size of data buffer does not match image dimensions.");
    }
    for (int i=0; i<width*height*bands; i++) {
      pixelArray.setElemFloat(i,data[i]);
    }
  }

  /**
   * Get object type identifier.
   *
   * @return Type of object.
   */
  public HipiImageType getType() {
    return HipiImageType.FLOAT;
  }

  /**
   * Provides direct access to underlying float array of pixel data.
   */
  public float[] getData() {
    return ((PixelArrayFloat)this.pixelArray).getData();
  }

  /**
   * Compares two ByteImage objects for equality allowing for some
   * amount of differences in pixel values.
   *
   * @return True if the two images have equal dimensions, color
   * spaces, and are found to deviate by less than a maximum
   * difference, false otherwise.
   */
  public boolean equalsWithTolerance(RasterImage thatImage, float maxDifference) {
    if (thatImage == null) {
      return false;
    }
    // Verify dimensions in headers are equal
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    if (this.getColorSpace() != thatImage.getColorSpace() ||
	thatImage.getWidth() != w || thatImage.getHeight() != h || 
	thatImage.getNumBands() != b) {
      return false;
    }

    // Get pointers to pixel arrays
    PixelArray thisPA = this.getPixelArray();
    PixelArray thatPA = thatImage.getPixelArray();

    // Check that pixel data is equal.
    for (int i=0; i<w*h*b; i++) {
      double diff = Math.abs(thisPA.getElemFloat(i)-thatPA.getElemFloat(i));
      if (diff > maxDifference) {
	return false;
      }
    }

    // Passed, declare equality
    return true;
  }

  /**
   * Compares two FloatImage objects for equality.
   *
   * @return True if the two images are found to deviate by less than
   * 1.0/255.0 at each pixel and across each band, false otherwise.
   */
  @Override
  public boolean equals(Object that) {
    // Check for pointer equivalence
    if (this == that)
      return true;

    // Verify object types are equal
    if (!(that instanceof FloatImage))
      return false;

    return equalsWithTolerance((FloatImage)that, 0.0f);
  }

  /**
   * Performs in-place addition with another {@link FloatImage}.
   * 
   * @param thatImage target image to add to current image
   *
   * @throws IllegalArgumentException if the image dimensions do not match
   */
  public void add(FloatImage thatImage) throws IllegalArgumentException {
    // Verify input
    checkCompatibleInputImage(thatImage);

    // Perform in-place addition
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    float[] thisData = this.getData();
    float[] thatData = thatImage.getData();
    
    for (int i=0; i<w*h*b; i++) {
      thisData[i] += thatData[i];
    }
  }

  /**
   * Performs in-place addition of a scalar to each band of every pixel.
   * 
   * @param number scalar value to add to each band of each pixel
   */
  public void add(float number) {
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    float[] thisData = this.getData();
    for (int i=0; i<w*h*b; i++) {
      thisData[i] += number;
    }
  }

  /**
   * Performs in-place elementwise multiplication of {@link FloatImage} and the current image.
   *
   * @param thatImage target image to use for multiplication
   */
  public void multiply(FloatImage thatImage) throws IllegalArgumentException {

    // Verify input
    checkCompatibleInputImage(thatImage);

    // Perform in-place elementwise multiply
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    float[] thisData = this.getData();
    float[] thatData = thatImage.getData();
    for (int i=0; i<w*h*b; i++) {
      thisData[i] *= thatData[i];
    }
  }

  /**
   * Performs in-place multiplication with scalar.
   *
   * @param value Scalar to multiply with each band of each pixel.
   */
  public void scale(float value) {
    int w = this.getWidth();
    int h = this.getHeight();
    int b = this.getNumBands();
    float[] thisData = this.getData();
    for (int i=0; i<w*h*b; i++) {
      thisData[i] *= value;
    }
  }

  /**
   * Computes hash of float array of image pixel data.
   *
   * @return Hash of pixel data represented as a string.
   *
   * @see ByteUtils#asHex is used to compute the hash.
   */
  @Override
  public String hex() {
    float[] pels = this.getData();
    return ByteUtils.asHex(ByteUtils.floatArrayToByteArray(pels));
  }

  /**
   * Helper routine that verifies two images have compatible
   * dimensions for common operations (addition, elementwise
   * multiplication, etc.)
   *
   * @param image RasterImage to check
   * 
   * @throws IllegalArgumentException if the image do not have
   * compatible dimensions. Otherwise has no effect.
   */
  protected void checkCompatibleInputImage(FloatImage image) throws IllegalArgumentException {
    if (image.getColorSpace() != this.getColorSpace() || image.getWidth() != this.getWidth() || 
	image.getHeight() != this.getHeight() || image.getNumBands() != this.getNumBands()) {
      throw new IllegalArgumentException("Color space and/or image dimensions do not match.");
    }
  }
  
  public static FloatImage gaussianFilter(int radius) {
    if (radius <= 0) {
      throw new IllegalArgumentException("Radius must be a positive integer.");
    }
    
    int dimension = 2 * radius + 1;
    FloatImage output = new FloatImage(dimension, dimension, 1);
    float[] data = output.getData();
    float totalWeight = 0.0f;
    
    // Fill each index with a weight computed from the gaussian function
    for (int y = 0; y < dimension; y++) {
      for (int x = 0; x < dimension; x++) {
        int deltaY = y - radius;
        int deltaX = x - radius;
        int i = x + y * dimension;
        float weight = (float) Math.exp(-(deltaX * deltaX + deltaY * deltaY) / (2.0d * radius * radius));
        data[i] = weight;
        totalWeight += weight;
      }
    }
    
    // Normalize
    for (int i = 0; i < data.length; i++) {
      float normWeight = data[i] / totalWeight;
      data[i] = normWeight;
    }
    
    return output;
  }
  
  public void convolution(FloatImage filter, FloatImage output) throws IllegalArgumentException{    
    // Verify the number of bands
    int filterBands = filter.getNumBands();
    if (filterBands != 1 && filterBands != this.getNumBands()) {
      throw new IllegalArgumentException(
          "The number of bands for the filter must be equal to the number of bands for the RasterImage or equal to 1.");
    }
    
    int h = this.getHeight();
    int w = this.getWidth();
    int b = this.getNumBands();
    
    // Verify output dimension
    if (output.getWidth() != w || output.getHeight() != h) {
      throw new IllegalArgumentException("Mismatch between the output dimension and the source dimension.");
    }
    
    // Verify colorspace
    if (this.getColorSpace() != output.getColorSpace()) {
      throw new IllegalArgumentException("Mismatch between the colorspace of the output and the source.");
    };
    
    // Verify square filter
    if (filter.getWidth() != filter.getHeight()) {
      throw new IllegalArgumentException("The filter's height and width must be the same.");
    }
    
    float[] srcData = this.getData();
    float[] outData = output.getData();
    
    float[] filterData = filter.getData();
    int filterSize = filter.getWidth();
    int filterCenter =  filterSize / 2;
    
    // Convolution Algorithm
    for (int y = 0; y < h; y++) {
      for (int x = 0; x < w; x++) {
        for (int c = 0; c < b; c++) {
          float totalValue = 0;
          int paIndex = c + (x  + y * w) * b;
          
          // loop through the filter
          for (int filterY = 0; filterY < filterSize; filterY++) {
            for (int filterX = 0; filterX < filterSize; filterX++) {
              int _y = y + filterY - filterCenter;
              int _x = x + filterX - filterCenter;
              if (_y < 0 || _x < 0 || _y >= h || _x >= w) {
                continue;
              }
              int filterC = c % filterBands;
              int filterIndex = filterC + (filterX + filterY * filterSize) * filterBands;
              int srcIndex = c + (_x + _y * w) * b;
              totalValue += filterData[filterIndex] * srcData[srcIndex];
            }
          }
          if (totalValue < 0) totalValue = 0;
          if (totalValue > 1) totalValue = 1;
          outData[paIndex] = totalValue;
          
        }
      }
    }
  }
  
  public void gpuConvolution(FloatImage filter, FloatImage output) {
    // Verify the number of bands
    int filterBands = filter.getNumBands();
    if (filterBands != 1 && filterBands != this.getNumBands()) {
      throw new IllegalArgumentException(
          "The number of bands for the filter must be equal to the number of bands for the RasterImage or equal to 1.");
    }
    
    int h = this.getHeight();
    int w = this.getWidth();
    int b = this.getNumBands();
    
    // Verify output dimension
    if (output.getWidth() != w || output.getHeight() != h || output.getNumBands() != b) {
      throw new IllegalArgumentException("Mismatch between the output dimension and the source dimension.");
    }
    
    // Verify colorspace
    if (this.getColorSpace() != output.getColorSpace()) {
      throw new IllegalArgumentException("Mismatch between the colorspace of the output and the source.");
    };
    
    // Verify square filter
    if (filter.getWidth() != filter.getHeight()) {
      throw new IllegalArgumentException("The filter's height and width must be the same.");
    }
    
    // Host machine's copy of the header information.
    float[] srcData = this.getData();
    int[] srcHeader = {w, h, b};
    float[] filterData = filter.getData();
    int[] filterHeader = {filter.getWidth(), filter.getHeight(), filter.getNumBands()};
    float[] destPels = output.getData();
    
    // Load the program source code.
    String programSrcCode = "" +
      "__kernel void convolveKernel(__global const float *srcData, __global const int *srcHeader," +
      "  __global const float *filterData, __global const int *filterHeader, __global float *destPels) {" +
      "  int id = get_global_id(0);" +
      "" +
      "  int srcWidth = srcHeader[0];" +
      "  int srcHeight = srcHeader[1];" +
      "  int srcBands = srcHeader[2];" +
      "  int filterWidth = filterHeader[0];" +
      "  int filterHeight = filterHeader[1];" +
      "  int filterBands = filterHeader[2];" +
      "  " +
      "  int filterCenterX = filterWidth / 2;" +
      "  int filterCenterY = filterHeight / 2;" +
      "  " +
      "  int c = id % srcBands;" +
      "  int x = (id / srcBands) % srcWidth;" +
      "  int y = id / (srcBands * srcWidth);" +
      "  " +
      "  destPels[id] = 0.0f;" +
      "  " +
      "  for (int filterY = 0; filterY < filterHeight; filterY++) {" +
      "    for (int filterX = 0; filterX < filterWidth; filterX++) {" +
      "      int dx = filterX - filterCenterX;" +
      "      int dy = filterY - filterCenterY;" +
      "      int srcX = x + dx;" +
      "      int srcY = y + dy;" +
      "" +
      "      if (srcX < 0 || srcX >= srcWidth || srcY < 0 || srcY >= srcHeight)" +
      "        continue;" +
      "" +
      "      int filterC = c % filterBands;" +
      "      int filterIndex = filterC + (filterX + filterY * filterWidth) * filterBands;" +
      "     int srcIndex = c + (srcX + srcY * srcWidth) * srcBands;" +
      "     " +
      "      destPels[id] += filterData[filterIndex] * srcData[srcIndex];" +
      "    }" +
      "  }" +
      "}";
    
    // OpenCL
    // Pointers.
    Pointer ptrSrcPels = Pointer.to(srcData);
    Pointer ptrSrcHeader = Pointer.to(srcHeader);
    Pointer ptrFilterPels = Pointer.to(filterData);
    Pointer ptrFilterHeader = Pointer.to(filterHeader);
    Pointer ptrDestPels = Pointer.to(destPels);

    // Enable exception.
    CL.setExceptionsEnabled(true);

    // Obtain a platform ID.
    int[] numPlatformsArray = new int[1];
    CL.clGetPlatformIDs(0, null, numPlatformsArray);
    int numPlatforms = numPlatformsArray[0];
    cl_platform_id[] platforms = new cl_platform_id[numPlatforms];
    CL.clGetPlatformIDs(platforms.length, platforms, null);
    cl_platform_id platform = platforms[0];

    // Initialize the context properties.
    cl_context_properties contextProperties = new cl_context_properties();
    contextProperties.addProperty(CL.CL_CONTEXT_PLATFORM, platform);

    // Obtain a device ID.
    int[] numDevicesArray = new int[1];
    CL.clGetDeviceIDs(platform, CL.CL_DEVICE_TYPE_GPU, 0, null, numDevicesArray);
    int numDevices = numDevicesArray[0];
    cl_device_id[] devices = new cl_device_id[numDevices];
    CL.clGetDeviceIDs(platform, CL.CL_DEVICE_TYPE_GPU, numDevices, devices, null);
    cl_device_id device = devices[0];

    // Create a context for the selected device.
    cl_context context =
        CL.clCreateContext(contextProperties, 1, new cl_device_id[] {device}, null, null, null);

    // Create a command-queue for the selected device.
    cl_command_queue commandQueue = CL.clCreateCommandQueue(context, device, 0, null);

    // Allocate the memory objects for the input and output data.
    cl_mem memObjects[] = new cl_mem[5];
    memObjects[0] =
        CL.clCreateBuffer(context, CL.CL_MEM_READ_ONLY | CL.CL_MEM_COPY_HOST_PTR, Sizeof.cl_float
            * srcData.length, ptrSrcPels, null);
    memObjects[1] =
        CL.clCreateBuffer(context, CL.CL_MEM_READ_ONLY | CL.CL_MEM_COPY_HOST_PTR, Sizeof.cl_int
            * srcHeader.length, ptrSrcHeader, null);
    memObjects[2] =
        CL.clCreateBuffer(context, CL.CL_MEM_READ_ONLY | CL.CL_MEM_COPY_HOST_PTR, Sizeof.cl_float
            * filterData.length, ptrFilterPels, null);
    memObjects[3] =
        CL.clCreateBuffer(context, CL.CL_MEM_READ_ONLY | CL.CL_MEM_COPY_HOST_PTR, Sizeof.cl_int
            * filterHeader.length, ptrFilterHeader, null);
    memObjects[4] =
        CL.clCreateBuffer(context, CL.CL_MEM_READ_WRITE, Sizeof.cl_float * destPels.length, null,
            null);

    // Create a program from the source code.
    cl_program program =
        CL.clCreateProgramWithSource(context, 1, new String[] {programSrcCode}, null, null);
    CL.clBuildProgram(program, 0, null, null, null, null);

    // Create kernel and set arguments.
    cl_kernel kernel = CL.clCreateKernel(program, "convolveKernel", null);
    CL.clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(memObjects[0]));
    CL.clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(memObjects[1]));
    CL.clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(memObjects[2]));
    CL.clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(memObjects[3]));
    CL.clSetKernelArg(kernel, 4, Sizeof.cl_mem, Pointer.to(memObjects[4]));

    // Set work-item dimension.
    long[] globalWorkSize = {destPels.length};
    long[] localWorkSize = {1};

    // Execute the kernel.
    CL.clEnqueueNDRangeKernel(commandQueue, kernel, 1, null, globalWorkSize, localWorkSize, 0,
        null, null);

    // Read the output data.
    CL.clEnqueueReadBuffer(commandQueue, memObjects[4], CL.CL_TRUE, 0, destPels.length
        * Sizeof.cl_float, ptrDestPels, 0, null, null);

    // Release kernel, program, and memory objects.
    CL.clReleaseMemObject(memObjects[0]);
    CL.clReleaseMemObject(memObjects[1]);
    CL.clReleaseMemObject(memObjects[2]);
    CL.clReleaseMemObject(memObjects[3]);
    CL.clReleaseMemObject(memObjects[4]);
    CL.clReleaseKernel(kernel);
    CL.clReleaseProgram(program);
    CL.clReleaseCommandQueue(commandQueue);
//    CL.clReleaseContext(context);
    // end of OpenCL
  }

} // public class FloatImage...
