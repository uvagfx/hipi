package org.hipi.tools.downloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DownloaderInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new DownloaderRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    Configuration conf = job.getConfiguration();

    // Use a default value of 10 if 'downloader.nodes' is not explicitly set
    int numDownloadNodes = conf.getInt("downloader.nodes", 10);

    // Initialize list to store unique nodes in cluster
    ArrayList<String> uniqueNodes = new ArrayList<String>(0);

    // Initialize list to store output InputSplits
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Create stub for temporary files
    FileSystem fileSystem = FileSystem.get(conf);
    String tempOutputPath = conf.get("downloader.outpath") + "_tmp";
    Path tempOutputDir = new Path(tempOutputPath);

    // Ensure clean temporary directory
    if (fileSystem.exists(tempOutputDir)) {
      fileSystem.delete(tempOutputDir, true);
    }
    fileSystem.mkdirs(tempOutputDir);

    // Search for numDownloadNodes unique nodes on the cluster by creating up to (2*numDownloadNodes) temporary files on the HDFS and seeing where they land.
    // Please visit http://hipi.cs.virginia.edu/examples/downloader.html for a detailed description.
    int i = 0;
    while (uniqueNodes.size() < numDownloadNodes && i < 2*numDownloadNodes) {

      // Create temporary file
      String tempFileString = tempOutputPath + "/" + i;
      Path tempFile = new Path(tempFileString);
      FSDataOutputStream os = fileSystem.create(tempFile);
      os.write(i);
      os.close();
      
      // Retrieve block locations of temporary file
      FileStatus match = fileSystem.getFileStatus(tempFile);
      long length = match.getLen();
      BlockLocation[] blocks = fileSystem.getFileBlockLocations(match, 0, length);

      // Check if the first node used to store this temporary file is not yet on our list
      boolean save = true;
      for (int j=0; j<uniqueNodes.size(); j++) {
        if (blocks[0].getHosts()[0].compareTo(uniqueNodes.get(j)) == 0) {
          save = false;
          System.out.println("Repeated host: " + i);
          break;
        }
      }

      // If unique, add it to list of unique nodes
      if (save) {
        uniqueNodes.add(blocks[0].getHosts()[0]);
        System.out.println("Unique host: " + i);
      }
      i++;

    }

    System.out.println("Tried to get " + numDownloadNodes + " unique nodes, found " + uniqueNodes.size() + " unique nodes.");

    boolean yfcc100m = conf.getBoolean("downloader.yfcc100m",false);

    // Accept an arbitrary number of input files for download, but assume one image/metadata set per line.
    for (FileStatus file : listStatus(job)) {

      Path path = file.getPath();

      // Determine number of images in file
      int numImages = 0;
      if (yfcc100m) {
        // Use filename to determine number of images in file
        String[] tokens = path.getName().split("-");
        if (tokens == null || tokens.length < 2) {
          throw new IOException("Unable to determine image count. Check that file name follows YFCC100M convention: " + path);
        }
        try {
          numImages = Integer.parseInt(tokens[1]);
        } catch (Exception e) {
          throw new IOException("Unable to determine image count. Check that file name follows YFCC100M convention: " + path);          
        }
      } else { // !yfcc100m
        // Determine number of images to download (assume a single input text file with one image URL per line)
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        while (reader.readLine() != null) {
          numImages++;
        }
        reader.close();      
      }

      System.out.println("Determined " + numImages + " images listed in: " + path.getName());

      // Determine download schedule (number of images per node)
      int span = (int) Math.ceil(((float) numImages) / ((float) uniqueNodes.size()));
      int last = numImages - span * (uniqueNodes.size() - 1);

      if (uniqueNodes.size() > 1) {
       System.out.println("First " + (uniqueNodes.size() - 1) + " nodes will each download " + span + " images");
       System.out.println("Last node will download " + last + " images");
     } else {
       System.out.println("Single node will download " + last + " images");
     }

      // Produce file splits according to download schedule
     for (int j = 0; j < uniqueNodes.size(); j++) {
       String[] node = new String[] {uniqueNodes.get(j)};
       if (j < uniqueNodes.size() - 1) {
         splits.add(new FileSplit(path, (j * span), span, node));
       } else {
         splits.add(new FileSplit(path, (j * span), last, node));
       }
     }

      // Remove temporary files used to identify unique nodes in cluster
     if (fileSystem.exists(tempOutputDir)) {
       fileSystem.delete(tempOutputDir, true);
     }
   }

    return splits;

  }

}
