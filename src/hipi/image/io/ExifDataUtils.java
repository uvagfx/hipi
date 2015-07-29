package hipi.image.io;

import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;

import java.io.InputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.metadata.IIOMetadata;

/**
 * Helper class for reading and displaying EXIF image data using the
 * classes in javax.imageio.*
 *
 * Code adapted from http://johnbokma.com/java/obtaining-image-metadata.html.
 */
public class ExifDataUtils {

  /**
   * Attempts to read and parse image EXIF data from InputStream.
   *
   * @param is InputStream to use for reading
   *  
   * @return initialized IIOMetadata object if successful, null
   * otherwise
   */
  public static IIOMetadata readExifData(InputStream is) throws IOException {
    
    ImageInputStream iis = ImageIO.createImageInputStream(is);
    if (iis == null) {
      return null;
    }
	
    IIOMetadata exifData = null;

    Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
    
    if (readers.hasNext()) {
      
      // pick the first available ImageReader
      ImageReader reader = readers.next();
      
      // attach source to the reader
      reader.setInput(iis, true);
      
      // read metadata of first image
      exifData = reader.getImageMetadata(0);
      
    }

    return exifData;
    
  }

  /**
   * Prints image EXIF data as DOM tree to standard output.
   *
   * @param exifData IIOMetadata object that contains image EXIF data
   */
  public static void displayExifData(IIOMetadata exifData) {
    String[] names = exifData.getMetadataFormatNames();
    int length = names.length;
    for (int i=0; i<length; i++) {
      System.out.println( "EXIF data format name: " + names[i]);
      displayExifData(exifData.getAsTree(names[i]), 0);
    }
  }

  private static void indent(int level) {
    for (int i = 0; i < level; i++)
      System.out.print("    ");
  }

  private static void displayExifData(Node node, int level) {
    // Print open tag of element
    indent(level);
    System.out.print("<" + node.getNodeName());
    NamedNodeMap map = node.getAttributes();
    if (map != null) {
      
      // Print attribute values
      int length = map.getLength();
      for (int i = 0; i < length; i++) {
	Node attr = map.item(i);
	System.out.print(" " + attr.getNodeName() +
			 "=\"" + attr.getNodeValue() + "\"");
      }
    }
    
    Node child = node.getFirstChild();
    if (child == null) {
      // No children exist, so close element and return
      System.out.println("/>");
      return;
    }
    
    // Children exist, so close current tag and descend
    System.out.println(">");
    while (child != null) {
      // Print children recursively
      displayExifData(child, level + 1);
      child = child.getNextSibling();
    }

    // Print close tag of element
    indent(level);
    System.out.println("</" + node.getNodeName() + ">");
  }

}
