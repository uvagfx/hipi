package hipi.imagebundle.mapreduce;

import java.io.IOException;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Provides the basic functionality of an ImageBundle record reader. Utilizes
 * {@link hipi.imagebundle.HipiImageBundle.FileReader} to read the portion of the HipiImageBundle
 * denoted by the InputSplit to get the ImageHeader and FloatImage
 * 
 *
 */
public class ImageBundleRecordReader<ImageType> extends RecordReader<ImageHeader, ImageType> {

  private Configuration conf;
  private HipiImageBundle.FileReader reader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

    FileSplit bundleSplit = (FileSplit)split;
    conf = context.getConfiguration();
    
    Path path = bundleSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    
    // Report locations of first and last byte in image segment
    System.out.println("Record starts at byte " + bundleSplit.getStart() + " and ends at byte " + (bundleSplit.getStart() + bundleSplit.getLength() - 1));
    
    reader = new HipiImageBundle<ImageType>.FileReader(fs, path, conf, bundleSplit.getStart(), bundleSplit.getStart() + bundleSplit.getLength() - 1);
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
  public FloatImage getCurrentValue() throws IOException, InterruptedException  {
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
