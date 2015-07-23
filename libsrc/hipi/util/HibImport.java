package hipi.util;

import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HibImport {

  public static void main(String[] args) throws IOException  {

    if (args.length < 2) {
      System.out.println("Usage: hibimport <input directory on local file system> <output HIB file on HDFS>");
      System.exit(0);
    }

    File folder = new File(args[0]);
    File[] files = folder.listFiles();

    if (files == null) {
      System.err.println(String.format("Did not find any files in the directory [%s]", args[0]));
      System.exit(0);
    }

    Configuration conf = new Configuration();
    HipiImageBundle hib = new HipiImageBundle(new Path(args[1]), conf);
    hib.openForWrite(true);

    for (File file : files) {
      FileInputStream fis = new FileInputStream(file);
      String localPath = file.getPath();
      HashMap<String, String> metaData = new HashMap<String,String>();
      metaData.put("source", localPath);
      String fileName = file.getName().toLowerCase();
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
    
    System.out.println("Created: " + args[1] + " and " + args[1] + ".dat");
  }

}
