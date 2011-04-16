package hipi.util;
import java.util.Comparator;

import hipi.container.HARIndexContainer;

public class HARIndexContainerSorter implements Comparator<HARIndexContainer>{

	public int compare(HARIndexContainer arg0, HARIndexContainer arg1) {
		return arg0.hash - arg1.hash;
	}

}
