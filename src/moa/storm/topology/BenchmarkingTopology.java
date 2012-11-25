package moa.storm.topology;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Map;

import moa.storm.scheme.InstanceScheme;
import moa.streams.generators.RandomTreeGenerator;
import moa.trident.state.jcs.JCSState;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

public class BenchmarkingTopology extends StormClusterTopology {

	@Override
	public StateFactory createFactory(String key) {
		return JCSState.create(key);
	}

	@Override
	public Stream createLearningStream(Map options, TridentTopology topology) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout spout = new MOAStreamSpout(new InstanceScheme(), stream);
		return topology.newStream("learning_instances", spout);
	}
	
	public Stream createPredictionStream(Map options, TridentTopology topology) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout spout = new MOAStreamSpout(new InstanceScheme(), stream);
		return topology.newStream("prediction_instances", spout);
	}

	private static final long serialVersionUID = 6105730415247268841L;

	public BenchmarkingTopology(String propertyFile) throws IOException {
		super(propertyFile);
	}
	
	public Filter outputQueue(Map options)
	{
		return new Filter(){
			long m_start =0;
			long m_measurement_start = 0;
			long count = 0;
			long period = 0;
			
			final long MEASUREMENT_DELAY = 5 * 60 * 1000;
			
			final long MEASUREMENT_PERIOD = 5 * 60 * 1000;
		
			@Override
			public void cleanup() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void prepare(Map arg0, TridentOperationContext arg1) {
				
				
			}

			@Override
			public boolean isKeep(TridentTuple arg0) {
				if (m_start == 0 ) 
					m_start = System.currentTimeMillis();
				if (System.currentTimeMillis() - m_start > MEASUREMENT_DELAY)
					count ++;
				if (System.currentTimeMillis() - m_start > MEASUREMENT_DELAY + MEASUREMENT_PERIOD)
				{
					writeResult();
					m_start = 0;
					period ++;
				}
				
				return false;
			}
			
			private void writeResult()
			{
				try {
					File f = new File("/tmp/trident_bench" + InetAddress.getLocalHost().getHostName() + "-" +period + "-"+ System.currentTimeMillis());
					FileOutputStream fos = new FileOutputStream(f);
					String result = "" + count;
					fos.write(result.getBytes());
					fos.flush();
					fos.close();
				}
				catch (Throwable t)
				{
					t.printStackTrace();
				}
			}
			
		};
	}

}
