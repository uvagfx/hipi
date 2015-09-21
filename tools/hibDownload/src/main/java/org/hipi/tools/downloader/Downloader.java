package org.hipi.tools.downloader;

import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.imagebundle.HipiImageBundle;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PngCodec;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A MapReduce program that takes a list of image URL's, downloads
 * them, and creates a {@link org.hipi.imagebundle.HipiImageBundle} from
 * them. Also supports downloading the Yahoo/Flickr 100M CC dataset.
 * 
 * When running this program, the user must specify 3 parameters. The
 * first is the location of the list of URL's (one URL per line), the
 * second is the output path for the HIB that will be generated, and
 * the third is the number of nodes that should be used during the
 * program's execution. This final parameter should be chosen with
 * respect to the total bandwidth your particular cluster is able to
 * handle. An example usage would be: 
 * <p>
 * downloader.jar /path/to/urls.txt /path/to/output.hib 10 
 * <p>
 * This program
 * will automatically force 10 nodes to download the set of URL's
 * contained in the input list, thus if your list contains 100,000
 * images, each node in this example will be responsible for
 * downloading 10,000 images.
 *
 */
public class Downloader extends Configured implements Tool {

  private static final Options options = new Options();
  private static final Parser parser = (Parser)new BasicParser();
  static {
    options.addOption("f", "force", false, "force overwrite if output HIB already exists");
    options.addOption("y", "yfcc100m", false, "assume input files are in Yahoo/Flickr CC 100M format");
    options.addOption("n", "num-nodes", true, "number of download nodes (default=1) (ignored if --yfcc100m is specified)");
  }

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(148);
    formatter.printHelp("hibDownload.jar <directory containing source files> <output HIB> [-f] [--yfcc100m] [--num-nodes #count]", options);
    System.exit(0);
  }

  private static long uniqueMapperKey = 0; // Ensures temp hib paths in mapper are unique
  private static long numDownloads = 0; // Keeps track of number of image downloads

  private final String FLICKR_PREFIX = "yfcc100m_dataset"; // This string represents the root name for each of the dataset files

  public static class DownloaderMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {

    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      // Use line number and a unique key assigned to each map task to generate a unique filename.
      String tempPath = conf.get("downloader.outpath") + key.get() + uniqueMapperKey +  ".hib.tmp";

      boolean yfcc100m = conf.getBoolean("downloader.yfcc100m", false);

      // Create new temporary HIB
      HipiImageBundle hib = new HipiImageBundle(new Path(tempPath), conf);
      hib.openForWrite(true);

      // The value argument contains a list of image URLs delimited by
      // '\n'. Setup buffered reader to allow processing this string
      // line by line.
      BufferedReader lineReader = new BufferedReader(new StringReader(value.toString()));
      String line;

      // Iterate through URLs
      while ((line = lineReader.readLine()) != null) {

        String[] lineFields = null;
        String imageUri = null;

        if (yfcc100m) {
          // Split line into fields
          lineFields = line.split("\t"); // Fields within each line are delimited by tabs  
          if (lineFields[22].equals("1")) { // 0 = image, 1 = video in YFCC100M format
            continue;
          }
          imageUri = lineFields[14];
        } else {
          imageUri = line; // Otherwise, assume entire line is image URL
        }

        long startTime = System.currentTimeMillis();
        try {

          String type = "";
          URLConnection conn;
                    
          // Attempt to download image at URL using java.net
          try {
            URL link = new URL(imageUri);
            numDownloads++;
            System.out.println("");
            System.out.println("Downloading: " + link.toString());
            System.out.println("Number of downloads: " + numDownloads);
            conn = link.openConnection();
            conn.connect();
            type = conn.getContentType();

            // Check that image format is supported, header is parsable, and add to HIB if so
            if (type != null && (type.compareTo("image/jpeg") == 0 || type.compareTo("image/png") == 0)) {
                            
              // Get input stream for URL connection
              InputStream bis = new BufferedInputStream(conn.getInputStream());
                            
              // Mark current location in stream for later reset
              bis.mark(Integer.MAX_VALUE);
    
              // Attempt to decode the image header
              HipiImageHeader header = (type.compareTo("image/jpeg") == 0 ? 
                JpegCodec.getInstance().decodeHeader(bis) : 
                PngCodec.getInstance().decodeHeader(bis));
                            
              if (header == null) {
                System.out.println("Failed to parse header, image not added to HIB: " + link.toString());
              } else {

                // Passed header decode test, so reset to beginning of stream
                bis.reset();
      
                if (yfcc100m) {
                  // Capture fields as image metadata for posterity
                  for (int i=0; i<lineFields.length; i++) {
                    header.addMetaData(String.format("col_%03d", i), lineFields[i]);
                  }
                  header.addMetaData("source", lineFields[14]);
                } else {
                  // Capture source URL as image metadata for posterity
                  header.addMetaData("source",imageUri);
                }
            
                // Add image to hib
                hib.addImage(header, bis);

                System.err.println("Added to HIB: " + imageUri);
              }
            } else {
              System.out.println("Unrecognized HTTP content type or unsupported image format [" + type + "], not added to HIB: " + imageUri);
            }
          } catch (Exception e) {
            System.out.println("Connection error while trying to download: " + imageUri);
            e.printStackTrace();
          }
        } catch (Exception e) {
          System.out.println("Network error while trying to download: " + imageUri);
          e.printStackTrace();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          }
        }

        float el = (float) (System.currentTimeMillis() - startTime) / 1000.0f;
        System.out.println("> Time elapsed: " + el + " seconds");

      } // while ((line = lineReader.readLine()) != null) {

      try {
        // Output key/value pair to reduce layer consisting of boolean and path to HIB
        context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
        // Cleanup
        lineReader.close();
        hib.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

      uniqueMapperKey++;

    }

    // Display metadata of the image
    public static void printFlickrImageMetadata(String[] lineArray) {
      System.out.println("  Flickr Image Metadata: ");
      System.out.println("    > Photo/Video Identifier: " + lineArray[0]);
      System.out.println("    > User NSID: " + lineArray[1]);
      System.out.println("    > User Nickname: " + lineArray[2]);
      System.out.println("    > Date Taken: " + lineArray[3]);
      System.out.println("    > Date Uploaded: " + lineArray[4]);
      System.out.println("    > Capture Device: " + lineArray[5]);
      System.out.println("    > Title: " + lineArray[6]);
      System.out.println("    > Description: " + lineArray[7]);
      System.out.println("    > User Tags: " + lineArray[8]);
      System.out.println("    > Machine Tags: " + lineArray[9]);
      System.out.println("    > Longitude: " + lineArray[10]);
      System.out.println("    > Latitude: " + lineArray[11]);
      System.out.println("    > Accuracy: " + lineArray[12]);
      System.out.println("    > Photo/Video Page URL: " + lineArray[13]);
      System.out.println("    > Photo/Video Download URL: " + lineArray[14]);
      System.out.println("    > License Name: " + lineArray[15]);
      System.out.println("    > License URL: " + lineArray[16]);
      System.out.println("    > Photo/Video Server Identifier: " + lineArray[17]);
      System.out.println("    > Photo/Video Farm Identifier: " + lineArray[18]);
      System.out.println("    > Photo/Video Secret: " + lineArray[19]);
      System.out.println("    > Photo/Video Secret Original: " + lineArray[20]);
      System.out.println("    > Extension of the Original Photo: " + lineArray[21]);
      System.out.println("    > Photos/video marker (0 = photo, 1 = video): " + lineArray[22]);
    }
  }

  public int run(String[] args) throws Exception {

    // try to parse command line arguments
    CommandLine line = null;
    try {
      line = parser.parse(options, args);
    }
    catch( ParseException exp ) {
      usage();
    }
    if (line == null) {
      usage();
    }

    String [] leftArgs = line.getArgs();

    if (leftArgs.length != 2) {
      usage();
    }

    String inputDir = leftArgs[0];
    String outputHib = leftArgs[1];

    boolean yfcc100m = line.hasOption("yfcc100m");
    int numDownloadNodes = (yfcc100m ? 1 : ((line.hasOption("num-nodes") ? Integer.parseInt(line.getOptionValue("num-nodes")) : 1)));
    if (numDownloadNodes < 1) {
      System.err.println("Invalid number of download nodes specified [" + numDownloadNodes + "]");
      System.exit(1);
    }

    boolean overwrite = line.hasOption("force");

    System.out.println("Source directory: " + inputDir);
    System.out.println("Output HIB: " + outputHib);
    System.out.println("Overwrite output HIB if it exists: " + (overwrite ? "true" : "false"));
    System.out.println("YFCC100M format: " + (yfcc100m ? "true" : "false"));
    System.out.println("Number of download nodes: " + numDownloadNodes);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // Remove existing HIB if overwrite is specified and HIB exists
    if (!overwrite) {
      if (fs.exists(new Path(outputHib))) {
        System.err.println("HIB [" + outputHib + "] already exists. Use the \"--force\" argument to overwrite.");
        System.exit(1);
      }
    } else { // overwrite
      if (fs.exists(new Path(outputHib))) {
        System.out.println("Found that output HIB already exists, deleting.");
      }
    }

    fs.delete(new Path(outputHib), true);
    fs.delete(new Path(outputHib+".dat"), true);
    fs.delete(new Path(outputHib+"_output"), true);

    // Scan source directory for list of input files
    FileStatus[] inputFiles = fs.listStatus(new Path(inputDir));
    if (inputFiles == null || inputFiles.length == 0) {
      System.err.println("Failed to find any files in source directory: " + inputDir);
      System.exit(1);
    }

    // Validate list of input files
    ArrayList<Path> sourceFiles = new ArrayList<Path>();
    for (FileStatus file : inputFiles) {

      Path path = file.getPath();

      if (yfcc100m) {
        String[] tokens = path.getName().split("-");
        if (tokens == null || tokens.length == 0) {
          System.out.println("  Skipping source file (does not follow YFCC100M file name convention): " + file.getPath());
          continue;
        }
      }

      try {
        // If it exists, get the relevant compression codec
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodec(path);

        FSDataInputStream fis = fs.open(path);

        // If the codec was found, use it to create an decompressed input stream.
        // Otherwise, assume input stream is already decompressed
        BufferedReader reader = null;
        if (codec != null) {
          reader = new BufferedReader(new InputStreamReader(codec.createInputStream(fis)));
        } else {
          reader = new BufferedReader(new InputStreamReader(fis));
        }

        String fileLine = reader.readLine();
        String[] lineFields = (yfcc100m ? fileLine.split("\t") : fileLine.split("\\s+"));

        if (yfcc100m) {
          if (lineFields.length != 23) {
            System.out.println("  Skipping source file (does not follow YFCC100M source file format): " + file.getPath());
            String imageUri = null;
          } else {
            System.out.println("  Adding source file: " + file.getPath());
            sourceFiles.add(path);
          }
        } else {
          if (lineFields.length != 1) {
            System.out.println("  Skipping source file (contains multiple fields per line where only one is expected): " + file.getPath());
            if (lineFields.length == 23) {
              System.out.println("  Did you mean to use \"--yfcc100m\"?");
            }
            String imageUri = null;
          } else {
            System.out.println("  Adding source file: " + file.getPath());
            sourceFiles.add(path);            
          }
        }
        fis.close();
        reader = null;
      } catch (Exception e) {
        System.err.println("Skipping source file (unable to open and parse first line: " + file.getPath());
        continue;
      }

    }

    if (sourceFiles.size() == 0) {
      System.err.println("Failed to find any valid files in source directory: " + inputDir);
      System.exit(1);
    }

    // Construct path to directory containing outputHib
    String outputPath = outputHib.substring(0, outputHib.lastIndexOf('/') + 1);

    // Attaching job parameters to global Configuration object
    conf.setInt("downloader.nodes", numDownloadNodes);
    conf.setStrings("downloader.outfile", outputHib);
    conf.setStrings("downloader.outpath", outputPath);
    conf.setBoolean("downloader.yfcc100m", yfcc100m);

    Job job = Job.getInstance(conf, "hibDownload");
    job.setJarByClass(Downloader.class);
    job.setMapperClass(DownloaderMapper.class);
    job.setReducerClass(DownloaderReducer.class);
    job.setInputFormatClass(DownloaderInputFormat.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    FileOutputFormat.setOutputPath(job, new Path(outputHib + "_output"));

    Path[] inputPaths = new Path[sourceFiles.size()];
    inputPaths = sourceFiles.toArray(inputPaths);
    DownloaderInputFormat.setInputPaths(job, inputPaths);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Downloader(), args);
    System.exit(res);
  }
}
