package hipi.unittest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	JPEGImageUtilTestCase.class,
	PNGImageUtilTestCase.class,
	HipiImageBundleTestCase.class,
})

public class AllTests {
}
