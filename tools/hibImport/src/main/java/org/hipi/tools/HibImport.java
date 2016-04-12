package org.hipi.tools;

import org.hipi.imagebundle.HipiImageBundle;
import org.hipi.image.HipiImageHeader.HipiImageFormat;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;

public class HibImport {

  private static final Options options = new Options();
  private static final Parser parser = (Parser)new BasicParser();
  static {
    options.addOption("f", "force", false, "force overwrite if output HIB already exists");
    options.addOption("h", "hdfs-input", false, "assume input directory is on HDFS");
  }

  private static void usage() {
    // usage
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hibImport.jar [options] <image directory> <output HIB>", options);
    System.exit(0);
  }

  public static void main(String[] args) throws IOException  {

    // Attempt to parse the command line arguments
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

    String imageDir = leftArgs[0];
    String outputHib = leftArgs[1];

    boolean overwrite = false;
    if (line.hasOption("f")) {
      overwrite = true;
    }

    boolean hdfsInput = false;
    if (line.hasOption("h")) {
      hdfsInput = true;
    }

    System.out.println("Input image directory: " + imageDir);
    System.out.println("Input FS: " + (hdfsInput ? "HDFS" : "local FS"));
    System.out.println("Output HIB: " + outputHib);
    System.out.println("Overwrite HIB if it exists: " + (overwrite ? "true" : "false"));

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (hdfsInput) {

      FileStatus[] files = fs.listStatus(new Path(imageDir));
      if (files == null) {
        System.err.println(String.format("Did not find any files in the HDFS directory [%s]", imageDir));
        System.exit(0);
      }
      Arrays.sort(files);

      HipiImageBundle hib = new HipiImageBundle(new Path(outputHib), conf);
      hib.openForWrite(overwrite);

      for (FileStatus file : files) {
        FSDataInputStream fdis = fs.open(file.getPath());
        String source = file.getPath().toString();
        HashMap<String, String> metaData = new HashMap<String,String>();
        metaData.put("source", source);
        String fileName = file.getPath().getName().toLowerCase();
        String suffix = fileName.substring(fileName.lastIndexOf('.'));
        if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
         hib.addImage(fdis, HipiImageFormat.JPEG, metaData);
         System.out.println(" ** added: " + fileName);
       } else if (suffix.compareTo(".png") == 0) {
         hib.addImage(fdis, HipiImageFormat.PNG, metaData);
         System.out.println(" ** added: " + fileName);
       }
     }

     hib.close();

    } else {

      File folder = new File(imageDir);
      File[] files = folder.listFiles();
      Arrays.sort(files);

      if (files == null) {
        System.err.println(String.format("Did not find any files in the local FS directory [%s]", imageDir));
        System.exit(0);
      }

      HipiImageBundle hib = new HipiImageBundle(new Path(outputHib), conf);
      hib.openForWrite(overwrite);

      for (File file : files) {
        FileInputStream fis = new FileInputStream(file);
        String localPath = file.getPath();
        HashMap<String, String> metaData = new HashMap<String,String>();
        metaData.put("source", localPath);
        String fileName = file.getName().toLowerCase();
        metaData.put("filename", fileName);
        String suffix = fileName.substring(fileName.lastIndexOf('.'));
        if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
         hib.addImage(fis, HipiImageFormat.JPEG, metaData);
         System.out.println(" ** added: " + fileName);
       }
       else if (suffix.compareTo(".png") == 0) {
         hib.addImage(fis, HipiImageFormat.PNG, metaData);
         System.out.println(" ** added: " + fileName);
       } 
     }

     hib.close();

    }

    
    System.out.println("Created: " + outputHib + " and " + outputHib + ".dat");
  }

}
