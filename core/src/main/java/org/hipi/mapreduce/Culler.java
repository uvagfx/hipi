package org.hipi.mapreduce;

import org.hipi.image.HipiImageHeader;

import java.lang.Object;

public class Culler extends Object {

  public static final String HIPI_CULLER_CLASS_ATTR = "hipi.culler.class";

  public boolean includeExifDataInHeader() {
    return false;
  }

  public boolean cull(HipiImageHeader header) {
    return false;
  }

}