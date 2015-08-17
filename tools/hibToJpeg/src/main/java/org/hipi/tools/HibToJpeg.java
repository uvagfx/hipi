package org.hipi.tools;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.ByteImage;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PngCodec;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.util.ByteUtils;

import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Iterator;
import java.io.IOException;

public class HibToJpeg extends Configured implements Tool {

  public static class HibToJpegMapper extends Mapper<HipiImageHeader, ByteImage, BooleanWritable, Text> {

    public Path path;
    public FileSystem fileSystem;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      fileSystem = FileSystem.get(context.getConfiguration());
      path = new Path(conf.get("jpegfromhib.outdir"));
      fileSystem.mkdirs(path);
    }

    /* 
     * Write each image (represented as an encoded byte array) to the
     * HDFS using the hash of the byte array to generate a unique
     * filename.
     */
    @Override
    public void map(HipiImageHeader header, ByteImage image, Context context) throws IOException, InterruptedException {

      // Check for null image (malformed HIB segment of failure to decode header)
      if (header == null || image == null) {
       System.err.println("Failed to decode image, skipping.");
       return;
      }

      String source = header.getMetaData("source");
      if (source == null) {
       System.err.println("Failed to locate source metadata key/value pair, skipping.");
       return;
      }
      
      String base = FilenameUtils.getBaseName(source);
      if (base == null) {
        System.err.println("Failed to determine base name of source metadata value, skipping.");
        return;
      }

      Path outpath = new Path(path + "/" + base + ".jpg");

      // Write image file to HDFS
      FSDataOutputStream os = fileSystem.create(outpath);
      JpegCodec.getInstance().encodeImage(image, os);
      os.flush();
      os.close();

      // Report success to reduce task
      context.write(new BooleanWritable(true), new Text(base));
    }
  }

  private static void removeDir(String pathToDirectory, Configuration conf) throws IOException {
    Path pathToRemove = new Path(pathToDirectory);
    FileSystem fileSystem = FileSystem.get(conf);
    if (fileSystem.exists(pathToRemove)) {
      fileSystem.delete(pathToRemove, true);
    }
  }

  public int run(String[] args) throws Exception {

    // Check arguments
    if (args.length != 2) {
      System.out.println("Usage: hibToJpeg.jar <input HIB> <output directory>");
      System.exit(0);
    }

    String inputPath = args[0];
    String outputPath = args[1];

    // Setup job configuration
    Configuration conf = new Configuration();
    conf.setStrings("jpegfromhib.outdir", outputPath);

    // Setup MapReduce classes
    Job job = Job.getInstance(conf, "jpegfromhib");
    job.setJarByClass(HibToJpeg.class);
    job.setMapperClass(HibToJpegMapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(HibInputFormat.class);

    job.setNumReduceTasks(1);

    // Clean up output directory
    removeDir(outputPath, conf);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    HibInputFormat.setInputPaths(job, new Path(inputPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HibToJpeg(), args);
    System.exit(res);
  }

}
