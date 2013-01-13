package moa.storm.topology;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

import moa.storm.scheme.InstanceScheme;
import moa.streams.generators.RandomTreeGenerator;
import moa.trident.state.jcs.JCSState;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

public class BenchmarkingTopology extends StormClusterTopology {

	static int INSTANCE = 0;
	public static Logger LOG = Logger.getLogger(BenchmarkingTopology.class);
	@Override
	public StateFactory createFactory(String key) {
		return JCSState.create(key);
	}

	@Override
	public Stream createLearningStream(Map options, TridentTopology topology) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout spout = new MOAStreamSpout(stream);
		return topology.newStream("learning_instances", spout);
	}
	
	public Stream createPredictionStream(Map options, TridentTopology topology) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		MOAStreamSpout spout = new MOAStreamSpout(stream);
		return topology.newStream("prediction_instances", spout);
	}

	private static final long serialVersionUID = 6105730415247268841L;

	public BenchmarkingTopology(String propertyFile) throws IOException {
		super(propertyFile);
	}
	
	public Function outputQueue(Map options)
	{
		return new Function(){
			long m_instance;
			long m_start =0;
			long m_measurement_start = 0;
			long count = 0;
			long period = 0;
			
			final long MEASUREMENT_DELAY = 1 * 60 * 1000;
			
			final long MEASUREMENT_PERIOD = 1 * 60 * 1000;
		
			@Override
			public void cleanup() {
				// TODO Auto-generated method stub
				
			}
			
			private int getPid() throws Throwable
			{
				java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
				java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
				jvm.setAccessible(true);
				sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
				java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
				pid_method.setAccessible(true);
				int pid = (Integer) pid_method.invoke(mgmt);
				return pid;
			}			

			@Override
			public void prepare(Map arg0, TridentOperationContext arg1) {
				m_instance = INSTANCE ++;
				System.out.println("New Instance "+ INSTANCE);
			}

			
			private void writeResult()
			{
				try {
					
					File f = new File("/home/vp37/trident_bench"+ InetAddress.getLocalHost().getHostName() + "-" + getPid() + "-" + m_instance);
					FileOutputStream fos = new FileOutputStream(f);
					String result = "" + count;
					fos.write(result.getBytes());
					fos.write(" \r\n".getBytes());
					fos.flush(); 
					fos.close();
				}
				catch (Throwable t)
				{
					t.printStackTrace();
				}
			}

			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				if (m_start == 0 ) 
					m_start = System.currentTimeMillis();
				if (System.currentTimeMillis() - m_start > MEASUREMENT_DELAY)
				{
					count ++;
					
				}
				if (System.currentTimeMillis() - m_start > MEASUREMENT_DELAY + MEASUREMENT_PERIOD)
				{
					LOG.info("Writing Result");
					writeResult();
					m_start = System.currentTimeMillis();
					period ++;
				}  
				
			}
			
		};
	}

}
