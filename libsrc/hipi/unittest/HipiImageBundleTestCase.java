package hipi.unittest;

import static org.junit.Assert.*;

import hipi.image.HipiImageFactory;
import hipi.image.ImageHeader.ImageFormat;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class HipiImageBundleTestCase extends AbstractImageBundleTestCase {

  @Override
  public AbstractImageBundle createImageBundleAndOpen(int mode, HipiImageFactory imageFactory) throws IOException {
    HipiImageBundle hib = new HipiImageBundle(imageFactory, new Path("/tmp/bundle.hib"), new Configuration());
    hib.open(mode, true);
    return hib;
  }

  @Test
  public void testOffsets() throws IOException {
    System.out.println("testOffsets");
    HipiImageBundle hib =
      (HipiImageBundle)createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ, null);
    Long trueOffsets[] = {2175094l, 6823622l, 9309561l, 12349434l, 14445862l, 19455737l, 20336265l, 21539565l, 21735915l, 21975786l, 23774727l};
    List<Long> offsets = hib.readAllOffsets();
    assertEquals(offsets.size(), trueOffsets.length);
    for (int i=0; i<trueOffsets.length; i++) {
      System.out.println(offsets.get(i));
      assertEquals(trueOffsets[i], offsets.get(i));
    }
  }

  @Test
  public void testAppend() throws IOException {
    // create image bundles
    Configuration conf = new Configuration();

    HipiImageBundle aib1 = new HipiImageBundle(null, new Path("/tmp/bundle1.hib"), conf);
    aib1.open(AbstractImageBundle.FILE_MODE_WRITE, true);
    aib1.addImage(new FileInputStream("testimages/01.JPEG"), ImageFormat.JPEG);
    aib1.addImage(new FileInputStream("testimages/02.JPG"), ImageFormat.JPEG);
    aib1.close();

    HipiImageBundle aib2 = new HipiImageBundle(null, new Path("/tmp/bundle2.hib"), conf);
    aib2.open(AbstractImageBundle.FILE_MODE_WRITE, true);
    aib2.addImage(new FileInputStream("testimages/03.jpg"), ImageFormat.JPEG);
    aib2.addImage(new FileInputStream("testimages/04.jpg"), ImageFormat.JPEG);
    aib2.close();

    HipiImageBundle aib1_in = new HipiImageBundle(null, new Path("/tmp/bundle1.hib"), conf);
    HipiImageBundle aib2_in = new HipiImageBundle(null, new Path("/tmp/bundle2.hib"), conf);

    HipiImageBundle merged_hib = new HipiImageBundle(null, new Path("/tmp/merged_bundle.hib"), conf);
    merged_hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
    merged_hib.append(aib1_in);
    merged_hib.append(aib2_in);
    merged_hib.close();
  }

}
