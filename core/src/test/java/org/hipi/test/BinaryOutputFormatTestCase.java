package org.hipi.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.hipi.mapreduce.BinaryOutputFormat;
import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.FloatBuffer;


public class BinaryOutputFormatTestCase {
  
  // Configuration objects for Record Writer
  private TaskAttemptContext context;
  private Job job;
  private Configuration conf;
  private FileSystem fileSystem;
  
  // Test data
  private NullWritable nullKey;
  private NullWritable nullValue;
  
  private static final int intKeyData = 0;
  private static final int intValueData = 1;
  private IntWritable intKey;
  private IntWritable intValue;
  
  private static final float[] cvValueData = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f};
  private static final int cvMatRows = 3;
  private static final int cvMatCols = 3;
  private OpenCVMatWritable cvValue;
  
  // I/O configuration
  private static final Path tempTestPath = new Path("file:///tmp/hipiBinaryOutputFormatTestPath");
  private static final Path tempOutputPath = new Path("file:///tmp/hipiBinaryOutputFormatTestPath/_temporary/0/_temporary/attempt_bof-test_0000_m_000000_0/part-m-00000");
  
  @Before
  public void setup() throws IOException {
    
    // Configure BinaryOutputFormat so that it emits proper RecordWriters
    job = Job.getInstance();
    conf = job.getConfiguration();
    conf.set("fs.defaultFS", "file:///");
    
    TaskAttemptID tID = new TaskAttemptID("bof-test", 0, TaskType.MAP, 0, 0);
    context = new TaskAttemptContextImpl(conf, tID);

    BinaryOutputFormat.setOutputPath(job, tempTestPath);
    
    // Ensure test path hasn't been previously created
    fileSystem = FileSystem.get(conf);
    if(fileSystem.exists(tempTestPath)) {
      fileSystem.delete(tempTestPath, true);
    }
    
    // Initialize data
    nullKey = NullWritable.get();
    nullValue = NullWritable.get();
    
    intKey = new IntWritable(intKeyData);
    intValue = new IntWritable(intValueData);
    
    Mat cvValueMat = new Mat(cvMatRows, cvMatCols, opencv_core.CV_32FC1, new Scalar(0.0));
    ((FloatBuffer)cvValueMat.createBuffer()).put(cvValueData);
    cvValue = new OpenCVMatWritable(cvValueMat);
    
  }
  
  @After
  public void teardown() throws IOException {
    
    //remove test path after testing has finished
    job = Job.getInstance();
    conf = job.getConfiguration();
    conf.set("fs.defaultFS", "file:///");
    fileSystem = FileSystem.get(conf);
    if(fileSystem.exists(tempTestPath)) {
      fileSystem.delete(tempTestPath, true);
    }
    
  }
  
  
  @Test
  public void testBinaryOutputFormatWithNonNullAndWritableKeyAndValue() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, IntWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, IntWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(intKey, intValue);
    writer.close(context);
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Keys are not equivalent.", intKey.get(), readIntWritable(dis).get());
      assertEquals("Values are not equivalent.", intValue.get(), readIntWritable(dis).get());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithNullWritableKey() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<NullWritable, IntWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<NullWritable, IntWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(nullKey, intValue);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Values are not equivalent.", intValue.get(), readIntWritable(dis).get());
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithNullWritableValue() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, NullWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, NullWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(intKey, nullValue);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Keys are not equivalent.", intKey.get(), readIntWritable(dis).get());
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithNullWritableKeyAndValue() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<NullWritable, NullWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<NullWritable, NullWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(nullKey, nullValue);
    writer.close(context);
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  
  @Test
  public void testBinaryOutputFormatWithNullKey() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, IntWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, IntWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(null, intValue);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Values are not equivalent.", intValue.get(), readIntWritable(dis).get());
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithNullValue() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, IntWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, IntWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(intKey, null);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Keys are not equivalent.", intKey.get(), readIntWritable(dis).get());
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
    
  }
  
  @Test
  public void testBinaryOutputFormatWithNullKeyAndValue() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, IntWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, IntWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(null, null);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithOpenCVMatWritableAndNullKey() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<NullWritable, OpenCVMatWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<NullWritable, OpenCVMatWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(nullKey, cvValue);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertArrayEquals("Mats are not equivalent.", cvValueData, readMatDataToArray(dis), 0.05f);
      assertEquals("Null values were written.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  @Test
  public void testBinaryOutputFormatWithOpenCVMatWritableAndIntKey() throws IOException, InterruptedException {
    @SuppressWarnings("unchecked")
    BinaryOutputFormat<IntWritable, OpenCVMatWritable> outputFormat = ReflectionUtils.newInstance(BinaryOutputFormat.class, conf);
    RecordWriter<IntWritable, OpenCVMatWritable> writer = outputFormat.getRecordWriter(context);
  
    writer.write(intKey, cvValue);
    writer.close(context);
    
    if(fileSystem.exists(tempOutputPath)) {
      FSDataInputStream dis = fileSystem.open(tempOutputPath);
      assertEquals("Keys are not equivalent.", intKey.get(), readIntWritable(dis).get());
      assertArrayEquals("Mats are not equivalent.", cvValueData, readMatDataToArray(dis), 0.05f);
      assertEquals("Not all data was read.", 0, dis.available());
    } else {
      fail("Output of record writer not located at: " + tempOutputPath + " as expected. Hadoop configuration may have changed.");
    }
  }
  
  private IntWritable readIntWritable(FSDataInputStream dis) throws IOException {
    IntWritable intWritable = new IntWritable();
    intWritable.readFields(dis);
    return intWritable;
  }
  
  private OpenCVMatWritable readOpenCVMatWritable(FSDataInputStream dis) throws IOException {
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable();
    openCVMatWritable.readFields(dis);
    return openCVMatWritable;
  }
  
  // Tests assume use of mats containing floats
  private float[] readMatDataToArray(FSDataInputStream dis) throws IOException {
    Mat newMat = readOpenCVMatWritable(dis).getMat();
    float[] newData = new float[cvValueData.length];
    ((FloatBuffer)newMat.createBuffer()).get(newData);
    return newData;
  }
  
  
  
}
