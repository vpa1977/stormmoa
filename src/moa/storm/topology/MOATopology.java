package moa.storm.topology;

import java.util.Map;

import storm.trident.TridentTopology;


public interface MOATopology {
	TridentTopology create(Map options);
}
