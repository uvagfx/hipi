package hipi.examples.jpegfromhib;

import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

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

public class JpegFromHib extends Configured implements Tool {

  public static class JpegFromHibMapper extends
      Mapper<ImageHeader, BytesWritable, BooleanWritable, Text> {

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
     * Write each image (represented as an encoded byte array) to the HDFS using the
     * hash of the byte array to generate a unique filename.
     *
     */
    @Override
    public void map(ImageHeader key, BytesWritable value, Context context) throws IOException,
        InterruptedException {

      // Check for null image (malformed HIB segment of failure to decode header)
      if (value == null) {
	System.err.println("Null byte array, skipping image."); 
        return;
      }

      // Determine file type
      String ext = "";
      if (key.getImageType() == ImageType.JPEG_IMAGE) {
	ext = ".jpg";
      } else if (key.getImageType() == ImageType.PNG_IMAGE) {
	ext = ".png";
      } else {
	System.err.println("Unsupported image type, skipping image.");
	return;
      }

      // Compute hash of byte stream to use as filename
      String hashval = ByteUtils.asHex(value.getBytes());
      Path outpath = new Path(path + "/" + hashval + ext);

      // Write image file to HDFS
      FSDataOutputStream os = fileSystem.create(outpath);
      os.write(value.getBytes());
      os.flush();
      os.close();

      // Report success to reduce task
      context.write(new BooleanWritable(true), new Text(hashval));
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
      System.out.println("Usage: jpegfromhib <input HIB> <output directory>");
      System.exit(0);
    }

    String inputPath = args[0];
    String outputPath = args[1];

    // Setup job configuration
    Configuration conf = new Configuration();
    conf.setStrings("jpegfromhib.outdir", outputPath);

    // Setup MapReduce classes
    Job job = Job.getInstance(conf, "jpegfromhib");
    job.setJarByClass(JpegFromHib.class);
    job.setMapperClass(JpegFromHibMapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    // Use JpegFromHibInputFormat instead of ImageBundleInputFormat to prevent the images
    // from being decoded
    job.setInputFormatClass(JpegFromHibInputFormat.class);

    job.setNumReduceTasks(1);

    // Clean up output directory
    removeDir(outputPath, conf);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    ImageBundleInputFormat.setInputPaths(job, new Path(inputPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JpegFromHib(), args);
    System.exit(res);
  }

}
