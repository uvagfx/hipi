package hipi.util.downloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as index into training array and value as the training vector.
 */
public class DownloaderRecordReader extends RecordReader<LongWritable, Text> {

  private boolean singletonEmit;
  private String urls;
  private long startLine;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

    // Obtain path to input list of image URLs
    FileSplit fileSplit = (FileSplit)split;
    Path path = fileSplit.getPath();
    FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

    // Note the start and length fields in the FileSplit object are being used to convey a
    // range of lines in the input list of image URLs
    startLine = fileSplit.getStart();
    long numLines = fileSplit.getLength();

    // Flag to enable emitting only one key/value pair
    singletonEmit = false;

    // Created buffered reader for input list of image URLs
    BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

    // Advance reader to startLine
    int i = 0;
    while (i < startLine && reader.readLine() != null) {
      i++;
    }

    // Build numLines length list of image URLs delimited by newline character \n
    urls = "";
    String line;
    for (i = 0; i < numLines && (line = reader.readLine()) != null; i++) {
      urls += line + '\n';
    }

    reader.close();
  }


  /**
   * Get the progress within the split
   */
  @Override
  public float getProgress() {
    if (singletonEmit) {
      return 1.0f;
    } else {
      return 0.0f;
    }
  }

  @Override
  public void close() throws IOException {
    return;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return new LongWritable((int) startLine);
  }


  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return new Text(urls);
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (singletonEmit == false) {
      singletonEmit = true;
      return true;
    } else {
      return false;
    }
  }


}
