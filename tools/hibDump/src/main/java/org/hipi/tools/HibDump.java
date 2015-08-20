package org.hipi.tools;

import org.hipi.image.FloatImage;
import org.hipi.image.ByteImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.util.ByteUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class HibDump extends Configured implements Tool {

  public static class HibDumpMapper extends Mapper<HipiImageHeader, ByteImage, IntWritable, Text> {
    
    @Override
    public void map(HipiImageHeader header, ByteImage image, Context context) throws IOException, InterruptedException  {

      String output = null;

      if (header == null) {
       output = "Failed to read image header.";
     } else if (image == null) {
       output = "Failed to decode image data.";
     } else {
       int w = header.getWidth();
       int h = header.getHeight();
       String source = header.getMetaData("source");
       String cameraModel = header.getExifData("Model");
       output = w + "x" + h + "\t(" + source + ")\t  " + cameraModel;
     }

     context.write(new IntWritable(1), new Text(output));
   }

  }
  
  public static class HibDumpReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
       context.write(key, value);
     }
   }

  }

  public int run(String[] args) throws Exception {
    
    if (args.length < 2) {
      System.out.println("Usage: hibDump <input HIB> <output directory>");
      System.exit(0);
    }

    Configuration conf = this.getConf();//new Configuration();

    Job job = Job.getInstance(conf, "hibDump");

    job.setJarByClass(HibDump.class);
    job.setMapperClass(HibDumpMapper.class);
    job.setReducerClass(HibDumpReducer.class);

    job.setInputFormatClass(HibInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    String inputPath = args[0];
    String outputPath = args[1];

    removeDir(outputPath, conf);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setNumReduceTasks(1);

    return job.waitForCompletion(true) ? 0 : 1;

  }

  private static void removeDir(String path, Configuration conf) throws IOException {
    Path output_path = new Path(path);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(output_path)) {
      fs.delete(output_path, true);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HibDump(), args);
    System.exit(res);
  }

}
