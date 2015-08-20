package org.hipi.tools.downloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class DownloaderRecordReader extends RecordReader<LongWritable, Text> {

 private long startLine;
 private long linesRead;
 private long numLines;
 private long linesPerRecord;
 private String urls;
 private BufferedReader reader;
    
 @Override
 public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

   // Obtain path to input list of input images and open input stream
   FileSplit fileSplit = (FileSplit)split;
   Path path = fileSplit.getPath();
   FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
   FSDataInputStream fileIn = fileSystem.open(path);
   
   // Note the start and length fields in the FileSplit object are being used to 
   // convey a range of lines in the input list of image URLs
   startLine = fileSplit.getStart();
   numLines = fileSplit.getLength();
   linesRead = 0; //total lines read by this particular record reader instance
   linesPerRecord = 100; //can be modified to change key/value pair size (may improve efficiency)
   
   //If it exists, get the relevant compression codec for the FileSplit
   CompressionCodecFactory codecFactory = new CompressionCodecFactory(context.getConfiguration());
   CompressionCodec codec = codecFactory.getCodec(path);
   
   // If the codec was found, use it to create an decompressed input stream.
   // Otherwise, assume input stream is already decompressed
   if (codec != null) {
     reader = new BufferedReader(new InputStreamReader(codec.createInputStream(fileIn)));
   } else {
     reader = new BufferedReader(new InputStreamReader(fileIn));
   }
   
 }

 // Get the progress within the split
 @Override
 public float getProgress() {
   float percent = (numLines == 0 ? 0.0f : ((float)linesRead)/((float)numLines));
   return percent;
 }
 
 @Override
 public void close() throws IOException {
   reader.close();
 }
 
 @Override
 public LongWritable getCurrentKey() throws IOException, InterruptedException {
   return new LongWritable(startLine + linesRead);
 }
 
 @Override
 public Text getCurrentValue() throws IOException, InterruptedException {
   return new Text(urls);
 }
 
 @Override
 public boolean nextKeyValue() throws IOException, InterruptedException {
   
   // if the record reader has reached the end of its partition, stop now.
   if (linesRead >= numLines) {
     return false;
   }
      
   urls = "";
   String line = "";
   
   // linesPerRecord is set in the initialize() method above.
   for (int i = 0; (i < linesPerRecord) && (linesRead < numLines); i++) {

     line = reader.readLine();

     if (line == null) {
       throw new IOException("Unexpected EOF while retrieving next line from input split.");
     }

     urls += line + "\n";
     linesRead++;
   }

   return !line.isEmpty();

 }

}
