package hipi.unittest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    FloatImageTestCase.class,
      JPEGImageUtilTestCase.class,
      PNGImageUtilTestCase.class,
      HipiImageBundleTestCase.class,
      SeqImageBundleTestCase.class
  //    HARImageBundleTestCase.class // No longer works in JUnit harness / requires full Hadoop environment
})

public class AllTests {
}
