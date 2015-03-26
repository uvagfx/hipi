package hipi.examples.jpegfromhib;

import hipi.image.ImageHeader;
import hipi.image.FloatImage;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JpegFromHibInputFormat extends FileInputFormat<ImageHeader, BytesWritable> {

  @Override
  public RecordReader<ImageHeader, BytesWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new JpegFromHibRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    // See ImageBundleInputFormat.java
    return ImageBundleInputFormat.computeSplits(jobContext, listStatus(jobContext));
  }

}
