package org.hipi.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class BinaryOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected static class BinaryRecordWriter<K, V> extends RecordWriter<K, V> {

    protected DataOutputStream out;

    public BinaryRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      out.close();
    }
    
    private void writeObject(Writable w) throws IOException {
      w.write(out);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      
      boolean nullKey = (key == null) || (key instanceof NullWritable);
      boolean nullValue = (value == null) || (value instanceof NullWritable);
      
      boolean writableKey = key instanceof Writable;
      boolean writableValue = value instanceof Writable;
      
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey && writableKey) {
        writeObject((Writable)key);
      }
      if (!nullValue && writableValue) {
        writeObject((Writable)value);
      }
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    boolean isCompressed = getCompressOutput(context);
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = 
          getOutputCompressorClass(context, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, context.getConfiguration());
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(context, extension);
    FileSystem fs = file.getFileSystem(context.getConfiguration());
    FSDataOutputStream fileOut = fs.create(file, false);
    if (!isCompressed) {
      return new BinaryRecordWriter<K, V>(fileOut);
    } else {
      return new BinaryRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
    }
  }

}