package hipi.util.downloader;

import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.image.io.JpegCodec;
import hipi.image.io.PngCodec;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

/**
 * A utility MapReduce program that downloads the Flickr 100 million
 * creative commons <a
 * href="http://yahoolabs.tumblr.com/post/89783581601/one-hundred-million-creative-commons-flickr-images">dataset</a>
 * into a {@link hipi.imagebundle.HipiImageBundle}. This program also
 * stores the metadata associated with each image in the HIB.
 * 
 * When running this program, the user must specify three
 * parameters. The first is the directory containing the Flickr
 * dataset; the second is the output path for the HIB that will be
 * generated. The third is the number nodes being requested for the
 * job.  An example usage would be:
 *
 * flickrDownloader.jar /path/to/flickr/directory /path/to/output.hib 100
 *
 * Note that the input directory should be left as it was downloaded
 * from Flickr/Yahoo. The dataset files should be left as .bz2
 * compressed files. Hadoop will accept these files in their
 * compressed state. The files should not be renamed, because this
 * example relies on the naming conventions specified by Flickr/Yahoo.
 */
public class FlickrDownloader extends Configured implements Tool {

    
  private final String FLICKR_DATA_NAME = "yfcc100m_dataset"; // This string represents the root name for each of the dataset files
  private final int FLICKR_IMAGE_COUNT_PER_FILE = 1000; // Number of images contained within each source file
  private final int FLICKR_DATA_COUNT = 2; // This parameter sets how many of the data files should be downloaded (used in strings which reference zero-indexed file names) (used for testing) 
  private final boolean FLICKR_TEST = true; // Set to true in order to use temporary, smaller dataset files for testing (used for testing)
  private static long uniqueMapperKey = 0; // Ensures that temp hib paths in mapper are unique (necessary when multiple input files are being used)
  private static long numDownloads = 0; // Counts number of downloads that have occurred (used for testing)
    
  // Download images from the input files and store them in a temporary HIB.
  public static class FlickrDownloaderMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {
        
    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                     
      // Use line number and a unique key assigned to each map task to generate a unique filename.
      String tempPath = conf.get("downloader.outpath") + key.get() + uniqueMapperKey +  ".hib.tmp";
          
      // Create new temporary HIB
      HipiImageBundle hib = new HipiImageBundle(new Path(tempPath), conf);
      //hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
      hib.openForWrite(true);

      // Parse mapper input (Text) into a list of strings
      String [] mapArray = value.toString().split("\n");

      // Parse lines from mapper array
      for (String line : mapArray) {
	
	// Split line into fields
	String[] lineArray = line.toString().split("\t"); // Fields within each line are delimited by tabs
	
	if (lineArray[22].equals("0")) { // 0 = image, 1 = video
	  
	  // Currently commented out because it floods sysout
	  //printImageMetadata(lineArray, hib); //currently prints the metadata to sysout - can be modified to append the data to the hib in future iterations
                
	  //download the image
	  long startT = 0;
	  long stopT = 0;
	  startT = System.currentTimeMillis();
	  try {
	    String type = "";
	    URLConnection conn;
                    
	    // Attempt to download image at URL using java.net
	    try {
	      URL link = new URL(lineArray[14]);
	      numDownloads++;
	      System.out.println("");
	      System.out.println("Downloading " + link.toString());
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
		  System.out.println("Failed to parse header, not added to HIB: " + lineArray[14]);
		} else {
		  
		  // Passed header decode test, so reset to beginning of stream
		  bis.reset();
		  
		  // Add image to hib
		  hib.addImage(bis, type.compareTo("image/jpeg") == 0 ? HipiImageFormat.JPEG : HipiImageFormat.PNG);
		}
	      } else {
		System.out.println("Unrecognized HTTP content type or unsupported image format [" + type + "], not added to HIB: " + lineArray[14]);
	      }
	    } catch (Exception e) {
	      System.out.println("Connection error while trying to download: " + lineArray[14]);
	      e.printStackTrace();
	    }
	  } catch (Exception e) {
	    System.out.println("Encountered network error while trying to download: " + lineArray[14]);
	    e.printStackTrace();
	    try {
	      Thread.sleep(1000);
	    } catch (InterruptedException ie) {
	      ie.printStackTrace();
	    }
	  }
	  stopT = System.currentTimeMillis();
	  float el = (float) (stopT - startT) / 1000.0f;
	  System.out.println("> Time elapsed: " + el + " seconds");
	}
      }

      try {
	// Output key/value pair to reduce layer consisting of boolean and path to HIB
	context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
        
	// Cleanup
	hib.close();
      } catch (Exception e) {
	e.printStackTrace();
      }
      
      uniqueMapperKey++;

    }

    // Display metadata of the image
    private void printImageMetadata(String[] lineArray, HipiImageBundle hib) {
      System.out.println("");
      System.out.println("Image Metadata: ");
      System.out.println("> Photo/Video Identifier: " + lineArray[0]);
      System.out.println("> User NSID: " + lineArray[1]);
      System.out.println("> User Nickname: " + lineArray[2]);
      System.out.println("> Date Taken: " + lineArray[3]);
      System.out.println("> Date Uploaded: " + lineArray[4]);
      System.out.println("> Capture Device: " + lineArray[5]);
      System.out.println("> Title: " + lineArray[6]);
      System.out.println("> Description: " + lineArray[7]);
      System.out.println("> User Tags: " + lineArray[8]);
      System.out.println("> Machine Tags: " + lineArray[9]);
      System.out.println("> Longitude: " + lineArray[10]);
      System.out.println("> Latitude: " + lineArray[11]);
      System.out.println("> Accuracy: " + lineArray[12]);
      System.out.println("> Photo/Video Page URL: " + lineArray[13]);
      System.out.println("> Photo/Video Download URL: " + lineArray[14]);
      System.out.println("> License Name: " + lineArray[15]);
      System.out.println("> License URL: " + lineArray[16]);
      System.out.println("> Photo/Video Server Identifier: " + lineArray[17]);
      System.out.println("> Photo/Video Farm Identifier: " + lineArray[18]);
      System.out.println("> Photo/Video Secret: " + lineArray[19]);
      System.out.println("> Photo/Video Secret Original: " + lineArray[20]);
      System.out.println("> Extension of the Original Photo: " + lineArray[21]);
      System.out.println("> Photos/video marker (0 = photo, 1 = video): " + lineArray[22]);
    }
  } 

  public static class FlickrDownloaderReducer extends Reducer<BooleanWritable, Text, BooleanWritable, Text> {
    private static Configuration conf;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }
    
    @Override
    public void reduce(BooleanWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.println("Usage: downloader <directory containing Flickr/Yahoo dataset source files> <output HIB> <number downloader nodes>");
      System.exit(0);
    }

    File[] inputDirFiles = new File(args[0]).listFiles();

    if (inputDirFiles == null || inputDirFiles.length == 0) {
      System.err.println("Failed to find any files in directory [" + args[0] + "]");
      System.exit(0);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    ArrayList<Path> sourceFiles = new ArrayList<Path>();
    int numImages = 0;

    for (File file : inputDirFiles) {

      if (file.isFile() && file.getName().startsWith(FLICKR_DATA_NAME)) {
	System.out.println("Will download images in: " + file.getPath());
	sourceFiles.add(new Path(file.getPath()));
	numImages += FLICKR_IMAGE_COUNT_PER_FILE;
      }

    }

    String outputFile = args[1];
    String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/') + 1);
    
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
    conf.setStrings("downloader.outfile", outputFile);
    conf.setStrings("downloader.outpath", outputPath);
    conf.setInt("downloader.imagesperfile", 10000000);
    //    conf.setBoolean("downloader.test", FLICKR_TEST);
    conf.setInt("downloader.nodes", Integer.parseInt(args[2]));
    
    //Necessary for logging when Hadoop is not running in standalone configuration
    //conf.set("mapreduce.framework.name", "yarn");
    //conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
    //conf.set("yarn.resourcemanager.webapp.address", "localhost:8088");
    //conf.set("yarn.resourcemanager.address", "localhost:8032");       
    
    Job job = Job.getInstance(conf, "FlickrDownloader");
    job.setJarByClass(FlickrDownloader.class);
    job.setMapperClass(FlickrDownloaderMapper.class);
    //    job.setReducerClass(FlickrDownloaderReducer.class);
    //    job.setInputFormatClass(FlickrDownloaderInputFormat.class);
    job.setReducerClass(DownloaderReducer.class);
    job.setInputFormatClass(DownloaderInputFormat.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    
    FileOutputFormat.setOutputPath(job, new Path(outputFile + "_output"));
    //    FlickrDownloaderInputFormat.setInputPaths(job, sourceFiles.toArray());
    DownloaderInputFormat.setInputPaths(job, (Path[])sourceFiles.toArray());
    
    return job.waitForCompletion(true) ? 0 : 1;  
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FlickrDownloader(), args);
    System.exit(res);        
  }
}
