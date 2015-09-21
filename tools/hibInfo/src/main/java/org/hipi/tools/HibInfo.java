package org.hipi.tools;

import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.PixelArray;
import org.hipi.image.RasterImage;
import org.hipi.imagebundle.HipiImageBundle;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;


public class HibInfo {

  private static final Options options = new Options();
  private static final Parser parser = (Parser)new BasicParser();
  static {
    options.addOption("e", "extract", true, "extract image to disk");
    options.addOption("m", "meta", true, "extract meta data value");
    options.addOption("x", "show-exif", false, "display any image EXIF data");
    options.addOption("h", "show-meta", false, "display any image meta data");
  }

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(148);
    formatter.printHelp("hibInfo.jar <input HIB> [--show-exif] [--show-meta] [#index [--extract file.png] [--meta key]]", options);
    System.exit(0);
  }

  private static void displayImageHeader(HipiImageHeader header, boolean showMeta, boolean showExif) {

    System.out.println(String.format("   %d x %d", header.getWidth(), header.getHeight()));
    System.out.println(String.format("   format: %d", header.getStorageFormat().toInteger()));
    
    if (showMeta) {
      HashMap<String, String> metaData = header.getAllMetaData();
      System.out.println("   meta: " + metaData);
    }
    
    if (showExif) {
      HashMap<String, String> exifData = header.getAllExifData();
      System.out.println("   exif: " + exifData);
    }

  }
  
  public static void main(String[] args) throws IOException  {

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

    if (!(leftArgs.length == 1 || leftArgs.length == 2)) {
      usage();
    }

    String inputHib = leftArgs[0];

    // Validate input HIB
    {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      if (!fs.exists(new Path(inputHib))) {
        System.err.println("HIB index file not found: " + inputHib);
        System.exit(1);
      }
      if (!fs.exists(new Path(inputHib+".dat"))) {
        System.err.println("HIB data file not found: " + inputHib + ".dat");
        System.exit(1);
      }
    }

    boolean showExif = line.hasOption("show-exif");
    boolean showMeta = line.hasOption("show-meta");

    System.out.println("Input HIB: " + inputHib);
    System.out.println("Display meta data: " + (showMeta ? "true" : "false"));
    System.out.println("Display EXIF data: " + (showExif ? "true" : "false"));

    int imageIndex = -1;
    String extractImagePath = null;
    String metaKey = null;

    if (leftArgs.length == 2) {
      
      // try to decode image index
      try {
	imageIndex = Integer.parseInt(leftArgs[1]);
      } catch (NumberFormatException ex) {
	System.err.println("Unrecognized image index: " + leftArgs[1]);
	usage();
      }

      if (line.hasOption("extract")) {
	extractImagePath = line.getOptionValue("extract");
	if (extractImagePath == null || extractImagePath.length() == 0) {
	  usage();
	}
      }
      
      if (line.hasOption("meta")) {
	metaKey = line.getOptionValue("meta");
	if (metaKey == null || metaKey.length() == 0) {
	  usage();
	}
      }

      System.out.println("Image index: " + imageIndex);
      System.out.println("Extract image path: " + (extractImagePath == null ? "none" : extractImagePath));
      System.out.println("Meta data key: " + (metaKey == null ? "none" : metaKey));

    }

    HipiImageBundle hib = null;
      try {
	hib = new HipiImageBundle(new Path(inputHib), new Configuration(), HipiImageFactory.getByteImageFactory());
	hib.openForRead((imageIndex == -1 ? 0 : imageIndex));
      } catch (Exception ex) {
	System.err.println(ex.getMessage());
	ex.printStackTrace();
	System.exit(0);
      }

    if (imageIndex == -1) {
      int count = 0;
      while (hib.next()) {
	System.out.println("IMAGE INDEX: " + count);
	HipiImageHeader header = hib.currentHeader();
	displayImageHeader(header, showMeta, showExif);
	count++;
      }
      if (imageIndex == -1) {
	System.out.println(String.format("Found [%d] images.", count));
      }
    } else {

      if (!hib.next()) {
	System.err.println(String.format("Failed to locate image with index [" + imageIndex + "]. Check that HIB contains sufficient number of images."));
	System.exit(0);
      }

      HipiImageHeader header = hib.currentHeader();
      displayImageHeader(header, showMeta, showExif);

      if (extractImagePath != null) {
	  
	String imageExtension = FilenameUtils.getExtension(extractImagePath);
	if (imageExtension == null) {
	  System.err.println(String.format("Failed to determine image type based on extension [%s]. Please provide a valid path with complete extension.", extractImagePath));
	  System.exit(0);
	}
	
	ImageOutputStream ios = null;
	try {
	  ios = ImageIO.createImageOutputStream(new File(extractImagePath));
	} catch (IOException ex) {
	  System.err.println(String.format("Failed to open image file for writing [%s]", extractImagePath));
	  System.exit(0);
	}
	Iterator<ImageWriter> writers = ImageIO.getImageWritersBySuffix(imageExtension);
	if (writers == null) {
	  System.err.println(String.format("Failed to locate encoder for image extension [%s]", imageExtension));
	  System.exit(0);
	}	    
	ImageWriter writer = writers.next();
	if (writer == null) {
	  System.err.println(String.format("Failed to locate encoder for image extension [%s]", imageExtension));
	  System.exit(0);
	}
	System.out.println("Using image encoder: " + writer);
	writer.setOutput(ios);
	
	HipiImage image = hib.currentImage();
	
	int w = image.getWidth();
	int h = image.getHeight();
	
	BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
	
	PixelArray pa = ((RasterImage)image).getPixelArray();
	int[] rgb = new int[w*h];
	for (int i=0; i<w*h; i++) {
	  
	  int r = pa.getElemNonLinSRGB(i*3+0);
	  int g = pa.getElemNonLinSRGB(i*3+1);
	  int b = pa.getElemNonLinSRGB(i*3+2);
	  
	  rgb[i] = (r << 16) | (g << 8) | b;
	}
	bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);
	
	ImageWriteParam param = writer.getDefaultWriteParam();
	IIOImage iioImage = new IIOImage(bufferedImage, null, null);
	writer.write(null, iioImage, param);
	
	System.out.println(String.format("Wrote [%s]", extractImagePath));
      }

      if (metaKey != null) {
       String metaValue = header.getMetaData(metaKey);
       if (metaValue == null) {
         System.out.println("Meta data key [" + metaKey + "] not found.");
       } else {
         System.out.println(metaKey + ": " + metaValue);
       }
      }

    }

    hib.close();

  }

}
