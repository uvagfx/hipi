package hipi.unittest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    PixelArrayTestCase.class,
    HipiImageBundleTestCase.class
    //    FloatImageTestCase.class,
    //    JpegCodecTestCase.class,
    //    PngCodecTestCase.class,
})

public class AllTests {
}
