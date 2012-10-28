package moa.storm.topology;

import java.io.IOException;
import java.util.Map;

import moa.storm.scheme.InstanceScheme;
import moa.streams.generators.RandomTreeGenerator;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class BenchmarkingTopology extends StormClusterTopology {

	@Override
	public StateFactory createFactory(String string) {
		// TODO Auto-generated method stub
		return super.createFactory(string);
	}

	@Override
	public Stream createLearningStream(Map options, TridentTopology topology) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		
		MOAStreamSpout spout = new MOAStreamSpout(new InstanceScheme(), stream);
		return topology.newStream("instances", spout);
	}

	private static final long serialVersionUID = 6105730415247268841L;

	public BenchmarkingTopology(String propertyFile) throws IOException {
		super(propertyFile);
	}
	

}
