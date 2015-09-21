package org.hipi.tools.downloader;

import org.hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DownloaderReducer extends Reducer<BooleanWritable, Text, BooleanWritable, Text> {
  
  private static Configuration conf;
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
  }
  
  // Combine HIBs produced by the map tasks into a single HIB
  @Override
  public void reduce(BooleanWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    
    if (key.get()) {
      
      // Get path to output HIB
      FileSystem fileSystem = FileSystem.get(conf);
      Path outputHibPath = new Path(conf.get("downloader.outfile"));
      
      // Create HIB for writing
      HipiImageBundle hib = new HipiImageBundle(outputHibPath, conf);
      hib.openForWrite(true);
      
      // Iterate over the temporary HIB files created by map tasks
      for (Text tempString : values) {
	
	// Open the temporary HIB file
	Path tempPath = new Path(tempString.toString());
	HipiImageBundle inputBundle = new HipiImageBundle(tempPath, conf);
	
	// Append temporary HIB file to output HIB (this is fast)
	hib.append(inputBundle);
	
	// Remove temporary HIB (both .hib and .hib.dat files)
	Path indexPath = inputBundle.getPath();
	Path dataPath = new Path(indexPath.toString() + ".dat");
	fileSystem.delete(indexPath, false);
	fileSystem.delete(dataPath, false);
	
	// Emit output key/value pair indicating temporary HIB has been processed
	Text outputPath = new Text(inputBundle.getPath().toString());
	context.write(new BooleanWritable(true), outputPath);
	context.progress();
      }
      
      // Finalize output HIB
      hib.close();
    }
  }

}


