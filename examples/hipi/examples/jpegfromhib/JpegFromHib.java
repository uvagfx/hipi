package hipi.examples.jpegfromhib;

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
import org.apache.hadoop.io.NullWritable;
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
      Mapper<NullWritable, BytesWritable, BooleanWritable, Text> {

    public Path path;
    public FileSystem fileSystem;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      fileSystem = FileSystem.get(context.getConfiguration());
      path = new Path(conf.get("jpegfromhib.outdir"));
      fileSystem.mkdirs(path);
    }

    /* In this example, the mapper creates a new output path for each BytesWritable object passed into it,
     * using the hashval to generate a unique path. The reduce component of this example is trivial and
     * doesn't require additional implementation.
     */
    @Override
    public void map(NullWritable key, BytesWritable value, Context context) throws IOException,
        InterruptedException {
      if (value == null) {
        return;
      }
      String hashval = ByteUtils.asHex(value.getBytes());
      Path outpath = new Path(path + "/" + hashval + ".jpg");
      FSDataOutputStream os = fileSystem.create(outpath);
      os.write(value.getBytes());
      os.flush();
      os.close();

      context.write(new BooleanWritable(true), new Text(hashval));
    }
  }

  private static void removeDir(String path, Configuration conf) throws IOException {
    Path output_path = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (fileSystem.exists(output_path)) {
      fileSystem.delete(output_path, true);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: jpegfromhib <hibfile> <output dir>");
      System.exit(0);
    }

    String inputPath = args[0];
    String outputPath = args[1];

    Configuration conf = new Configuration();
    conf.setStrings("jpegfromhib.outdir", outputPath);

    Job job = Job.getInstance(conf, "jpegfromhib");
    job.setJarByClass(JpegFromHib.class);
    job.setMapperClass(JpegFromHibMapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(JpegFromHibInputFormat.class);
    job.setNumReduceTasks(1);

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
