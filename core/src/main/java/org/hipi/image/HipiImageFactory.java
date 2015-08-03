package org.hipi.image;

import org.hipi.image.HipiImage;
import org.hipi.image.HipiImage.HipiImageType;

import org.apache.hadoop.mapreduce.Mapper;

import java.lang.reflect.Method;

/**
 * Interface for creating concreate objects derived from the abstract
 * HipiImage base class as requested by Mapper. Uses Java reflection
 * utils to interrogate the map method in the Mapper class in order to
 * determine appropriate type.
 * @see hipi.imagebundle.mapreduce.HibRecordReader
 */

public class HipiImageFactory {

  private static final HipiImageFactory staticFloatImageFactory = new HipiImageFactory(HipiImageType.FLOAT);

  public static HipiImageFactory getFloatImageFactory() {
    return staticFloatImageFactory;
  }

  private static final HipiImageFactory staticByteImageFactory = new HipiImageFactory(HipiImageType.BYTE);

  public static HipiImageFactory getByteImageFactory() {
    return staticByteImageFactory;
  }

  private Class<?> imageClass = null;
  private HipiImageType imageType = HipiImageType.UNDEFINED;

  public HipiImageFactory(Class<? extends Mapper<?,?,?,?>> mapperClass)
    throws InstantiationException,
	   IllegalAccessException,
	   ExceptionInInitializerError,
	   SecurityException,
	   RuntimeException {

    findImageClass(mapperClass);
    
    HipiImage image = (HipiImage)imageClass.newInstance();
    imageType = image.getType();
  }

  public HipiImageFactory(HipiImageType imageType)
    throws IllegalArgumentException {

	// Call appropriate decode function based on type of image object
      switch (imageType) {
	case FLOAT:
	  imageClass = FloatImage.class;
	  break;
	case BYTE:
	  imageClass = ByteImage.class;
	  break;
	case RAW:
	  throw new IllegalArgumentException("Support for RAW image type not yet implemented.");
	case OPENCV:
	  throw new IllegalArgumentException("Support for OPENCV image type not yet implemented.");
	case UNDEFINED:
	default:
	  throw new IllegalArgumentException("Unexpected image type. Cannot proceed.");
	}

      this.imageType = imageType;
  }

  private void findImageClass(Class<? extends Mapper<?,?,?,?>> mapperClass) 
    throws SecurityException,
	   RuntimeException {

    for (Method method : mapperClass.getMethods()) {
      // Find map method (there will be at least two: one in concrete
      // base class and one in abstract Mapper superclass)
      if (!method.getName().equals("map")) {
	continue;
      }
      
      // Get parameter list of map method
      Class<?> params[] = method.getParameterTypes();
      if (params.length != 3) {
	continue;
      }

      // Use the fact that first parameter should be ImageHeader
      // object to identify target map method
      if (params[0] != HipiImageHeader.class) {
	continue;
      }
      
      // Store pointer to requested image class
      imageClass = params[1];
    }
    
    if (imageClass == null) {
      throw new RuntimeException("Failed to determine image class used in mapper (second argument in map method).");
    }

    if (!HipiImage.class.isAssignableFrom(imageClass)) {
      throw new RuntimeException("Found image class [" + imageClass + "], but it's not derived from HipiImage as required.");
    }

  }

  public HipiImageType getType() {
    return imageType;
  }
    
  public HipiImage createImage(HipiImageHeader imageHeader)
    throws InstantiationException,
	   IllegalAccessException,
	   ExceptionInInitializerError,
	   SecurityException,
	   IllegalArgumentException {
    
    HipiImage image = (HipiImage)imageClass.newInstance();
    image.setHeader(imageHeader);
    return image;
    
  }

}
