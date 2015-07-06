package hipi.imagebundle.mapreduce;

import hipi.image.HipiImage;
import hipi.image.FloatImage;
import hipi.image.ByteImage;
import hipi.image.HipiImageFactory;
import hipi.image.ImageHeader;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Main RecordReader class for HIB files. Utilizes {@link
 * hipi.imagebundle.HipiImageBundle.HibReader} to read and decode
 * portion of input HIB indicated by InputSplit. Also determines the
 * requested image type (the second "value" argument to the map method
 * in the current Mapper class) dynamically using Java reflection
 * utils.
 */
public class HibRecordReader extends RecordReader<ImageHeader, HipiImage> {

  private Configuration conf;
  private HipiImageBundle.HibReader reader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

    HipiImageFactory imageFactory = null;
    try {
      imageFactory = new HipiImageFactory(context.getMapperClass());
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
      ex.printStackTrace();
      System.exit(1);
    }

    FileSplit bundleSplit = (FileSplit)split;
    conf = context.getConfiguration();
    
    Path path = bundleSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    
    // Report locations of first and last byte in image segment
    System.out.println("HibRecordReader#initialize: Input split starts at byte offset " + bundleSplit.getStart() +
		       " and ends at byte offset " + (bundleSplit.getStart() + bundleSplit.getLength() - 1));
    
    reader = new HipiImageBundle.HibReader(imageFactory, fs, path, bundleSplit.getStart(), bundleSplit.getStart() + bundleSplit.getLength() - 1);
  }
  
  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public ImageHeader getCurrentKey() throws IOException, InterruptedException  {
    return reader.getCurrentKey();
  }

  @Override
  public HipiImage getCurrentValue() throws IOException, InterruptedException  {
    return reader.getCurrentValue();
  }
  
  @Override
  public float getProgress() throws IOException  {
    return reader.getProgress();
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException  {
    return reader.nextKeyValue();
  }
}
