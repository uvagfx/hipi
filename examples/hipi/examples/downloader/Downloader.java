package hipi.examples.downloader;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.HipiImageBundle;

import hipi.image.ImageHeader;
import hipi.image.io.JPEGImageUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;

/**
 * A utility MapReduce program that takes a list of image URL's, downloads them, and creates a
 * {@link hipi.imagebundle.HipiImageBundle} from them.
 * 
 * When running this program, the user must specify 3 parameters. The first is the location of the
 * list of URL's (one URL per line), the second is the output path for the HIB that will be
 * generated, and the third is the number of nodes that should be used during the program's
 * execution. This final parameter should be chosen with respect to the total bandwidth your
 * particular cluster is able to handle. An example usage would be: <br />
 * <br />
 * downloader.jar /path/to/urls.txt /path/to/output.hib 10 <br />
 * <br />
 * This program will automatically force 10 nodes to download the set of URL's contained in the
 * input list, thus if your list contains 100,000 images, each node in this example will be
 * responsible for downloading 10,000 images.
 *
 */
public class Downloader extends Configured implements Tool {

  public static class DownloaderMapper extends Mapper<IntWritable, Text, BooleanWritable, Text> {

    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    // Download images at the list of input URLs and store them in a temporary HIB.
    @Override
    public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

      // Create path for temporary HIB file
      String temp_path = conf.get("downloader.outpath") + key.get() + ".hib.tmp";
      HipiImageBundle hib = new HipiImageBundle(new Path(temp_path), conf);
      hib.open(HipiImageBundle.FILE_MODE_WRITE, true);

      // The value argument contains a list of image URLs delimited by \n. Setup buffered reader to allow processing this string line by line.
      BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
      String uri;
      int i = key.get();
      int iprev = i;

      // Iterate through URLs
      while ((uri = reader.readLine()) != null) {

	// Put at most 100 images in a temporary HIB
        if (i >= iprev + 100) {
          hib.close();
          context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
          temp_path = conf.get("downloader.outpath") + i + ".hib.tmp";
          hib = new HipiImageBundle(new Path(temp_path), conf);
          hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
          iprev = i;
        }

	// Setup to time download
        long startT = 0;
        long stopT = 0;
        startT = System.currentTimeMillis();

        // Perform download and update HIB
        try {

          String type = "";
          URLConnection conn;

	  // Attempt to download image at URL using java.net.URL
          try {
            URL link = new URL(uri);
            System.err.println("Downloading " + link.toString());
            conn = link.openConnection();
            conn.connect();
            type = conn.getContentType();
          } catch (Exception e) {
            System.err.println("Connection error while trying to download: " + uri);
            continue;
          }

	  // Check that image format is supported, header is parsable, and add to HIB if so
          if (type != null && (type.compareTo("image/jpeg") == 0 || type.compareTo("image/png") == 0)) {
	    ImageHeader header = JPEGImageUtil.getInstance().decodeImageHeader(conn.getInputStream());
	    if (header == null)  {
	      System.err.println("Failed to parse header, not added to HIB: " + uri);
	    } else {
	      hib.addImage(conn.getInputStream(), type.compareTo("image/jpeg") == 0 ? ImageType.JPEG_IMAGE : ImageType.PNG_IMAGE);
	      System.err.println("Added to HIB: " + uri);
	    }
          } else {
	    System.err.println("Unrecognized HTTP content type or unsupported image format [" + type + "], not added to HIB: " + uri);
	  }

        } catch (Exception e) {
          e.printStackTrace();
          System.err.println("Encountered network error while trying to download: " + uri);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          }
        }

        i++;

        // Report success and elapsed time
        stopT = System.currentTimeMillis();
        float el = (float) (stopT - startT) / 1000.0f;
        System.err.println("> Time elapsed " + el + " seconds");
      }

      try {

	// Output key/value pair to reduce layer consisting of boolean and path to HIB
        context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));

	// Cleanup
        reader.close();
        hib.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
  }

  public static class DownloaderReducer extends
      Reducer<BooleanWritable, Text, BooleanWritable, Text> {

    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    //combine mapper HipiImageBundles into single HipiImageBundle
    @Override
    public void reduce(BooleanWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      
      if (key.get()) {
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputHibPath = new Path(conf.get("downloader.outfile"));
        HipiImageBundle hib = new HipiImageBundle(outputHibPath, conf);
        hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
        for (Text temp_string : values) {
          Path temp_path = new Path(temp_string.toString());
          HipiImageBundle input_bundle = new HipiImageBundle(temp_path, conf);
          hib.append(input_bundle);
          Path index_path = input_bundle.getPath();
          Path data_path = new Path(index_path.toString() + ".dat");
          fileSystem.delete(index_path, false);
          fileSystem.delete(data_path, false);
          Text outputPath = new Text(input_bundle.getPath().toString());
          context.write(new BooleanWritable(true), outputPath);
          context.progress();
        }
        hib.close();
      }
    }
  }


  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.println("Usage: downloader <input file> <output file> <nodes>");
      System.exit(0);
    }

    String inputFile = args[0];
    String outputFile = args[1];
    String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/') + 1);
    int nodes = Integer.parseInt(args[2]);
    
    Configuration conf = new Configuration();
    
    //Attaching constant values to Configuration
    conf.setInt("downloader.nodes", nodes);
    conf.setStrings("downloader.outfile", outputFile);
    conf.setStrings("downloader.outpath", outputPath);

    Job job = Job.getInstance(conf, "Downloader");
    job.setJarByClass(Downloader.class);
    job.setMapperClass(DownloaderMapper.class);
    job.setReducerClass(DownloaderReducer.class);
    job.setInputFormatClass(DownloaderInputFormat.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    FileOutputFormat.setOutputPath(job, new Path(outputFile + "_output"));

    DownloaderInputFormat.setInputPaths(job, new Path(inputFile));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Downloader(), args);
    System.exit(res);
  }
}
