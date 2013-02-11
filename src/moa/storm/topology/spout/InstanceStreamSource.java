package moa.storm.topology.spout;

import java.util.ArrayList;

import weka.core.Instance;

public interface InstanceStreamSource {
	ArrayList<Instance> read();
}
