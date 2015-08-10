package org.hipi.mapreduce;

import org.hipi.image.HipiImageHeader;

import java.lang.Object;

/**
 * Class that extends the MapReduce framework and allows culling images from a HIB at run-time,
 * before they are fully decoded and delivered to the Mapper, in order to achieve more efficient
 * processing.
 */
public class Culler extends Object {

  public static final String HIPI_CULLER_CLASS_ATTR = "hipi.culler.class";

  public boolean includeExifDataInHeader() {
    return false;
  }

  public boolean cull(HipiImageHeader header) {
    return false;
  }

}