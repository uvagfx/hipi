package hipi.util.downloader;

import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.imagebundle.HipiImageBundle;
import hipi.image.HipiImageHeader;
import hipi.image.io.JpegCodec;
import hipi.image.io.PngCodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;

/**
 * A MapReduce program that takes a list of image URL's, downloads
 * them, and creates a {@link hipi.imagebundle.HipiImageBundle} from
 * them.
 * 
 * When running this program, the user must specify 3 parameters. The
 * first is the location of the list of URL's (one URL per line), the
 * second is the output path for the HIB that will be generated, and
 * the third is the number of nodes that should be used during the
 * program's execution. This final parameter should be chosen with
 * respect to the total bandwidth your particular cluster is able to
 * handle. An example usage would be: <br /> <br /> downloader.jar
 * /path/to/urls.txt /path/to/output.hib 10 <br /> <br /> This program
 * will automatically force 10 nodes to download the set of URL's
 * contained in the input list, thus if your list contains 100,000
 * images, each node in this example will be responsible for
 * downloading 10,000 images.
 *
 */
public class Downloader extends Configured implements Tool {

  public static class DownloaderMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {

    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    // Download images at the list of input URLs and store them in a temporary HIB.
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      System.out.println("value: " + value);

      // Create path for temporary HIB file
      String tempPath = conf.get("downloader.outpath") + key.get() + ".hib.tmp";
      HipiImageBundle hib = new HipiImageBundle(new Path(tempPath), conf);
      hib.openForWrite(true);

      // The value argument contains a list of image URLs delimited by
      // '\n'. Setup buffered reader to allow processing this string
      // line by line.
      BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
      String uri;
      long i = key.get();
      long iprev = i;

      // Iterate through URLs
      while ((uri = reader.readLine()) != null) {

	// Put at most 100 images in a temporary HIB
        if (i >= iprev + 100l) {
          hib.close();
          context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
          tempPath = conf.get("downloader.outpath") + i + ".hib.tmp";
          hib = new HipiImageBundle(new Path(tempPath), conf);
          hib.openForWrite(true);
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

	  // Attempt to download image at URL using java.net
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

	    // Get input stream for URL connection
	    InputStream bis = new BufferedInputStream(conn.getInputStream());

	    // Mark current location in stream for later reset
	    bis.mark(Integer.MAX_VALUE);

	    // Attempt to decode the image header
	    HipiImageHeader header = (type.compareTo("image/jpeg") == 0 ? JpegCodec.getInstance().decodeHeader(bis) : PngCodec.getInstance().decodeHeader(bis));

	    if (header == null)  {
	      System.err.println("Failed to parse header, not added to HIB: " + uri);
	    } else {

	      // Passed header decode test, so reset to beginning of stream
	      bis.reset();

	      // Add source URL as metadata for posterity
	      header.addMetaData("source",uri);

	      System.out.println(header);

	      // Add image to HIB
	      hib.addImage(header, bis);

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

  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.println("Usage: downloader <input text file with list of URLs> <output HIB> <number of download nodes>");
      System.exit(0);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    String inputFile = args[0];

    // Verify input and determine number of images
    int numImages = 0;
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0]))));
      while (br.readLine() != null) {
	numImages++;
      }
      br.close();
    } catch (IOException ex) {
      System.err.println("Failed to open file for reading [" + args[0] + "].");
      ex.printStackTrace();
      System.exit(0);
    }

    System.out.println("Will attempt to download " + numImages + " images.");

    String outputFile = args[1];
    String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/') + 1);
    int nodes = Integer.parseInt(args[2]);

    //clear duplicates before running job
    if(fs.exists(new Path(outputFile))) {
      fs.delete(new Path(outputFile), true);
    }
    if(fs.exists(new Path(outputFile+".dat"))) {
      fs.delete(new Path(outputFile+".dat"), true);
    }
    if(fs.exists(new Path(outputFile+"_output"))) {
      fs.delete(new Path(outputFile+"_output"), true);
    }

    //Attaching constant values to Configuration
    conf.setInt("downloader.nodes", nodes);
    conf.setStrings("downloader.outfile", outputFile);
    conf.setStrings("downloader.outpath", outputPath);
    conf.setInt("downloader.imagesperfile", numImages);

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
