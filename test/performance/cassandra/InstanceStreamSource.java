package performance.cassandra;

import java.util.ArrayList;

import weka.core.Instance;

public interface InstanceStreamSource {
	ArrayList<Instance> read();
}
